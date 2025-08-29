import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional

import requests

from common.Logger import logger
from common.config import Config
from utils.file_manager import file_manager, checkpoint


class SyncUtils:
    """同步工具类，负责异步发送keys到外部应用"""

    def __init__(self):
        """初始化同步工具"""
        # Gemini Balancer 配置
        self.balancer_url = Config.GEMINI_BALANCER_URL.rstrip('/') if Config.GEMINI_BALANCER_URL else ""
        self.balancer_auth = Config.GEMINI_BALANCER_AUTH
        self.balancer_sync_enabled = Config.parse_bool(Config.GEMINI_BALANCER_SYNC_ENABLED)
        self.balancer_enabled = bool(self.balancer_url and self.balancer_auth and self.balancer_sync_enabled)

        # GPT Load Balancer 配置
        self.gpt_load_url = Config.GPT_LOAD_URL.rstrip('/') if Config.GPT_LOAD_URL else ""
        self.gpt_load_auth = Config.GPT_LOAD_AUTH
        # 解析多个group names (逗号分隔)
        self.gpt_load_group_names = [name.strip() for name in Config.GPT_LOAD_GROUP_NAME.split(',') if name.strip()] if Config.GPT_LOAD_GROUP_NAME else []
        self.gpt_load_sync_enabled = Config.parse_bool(Config.GPT_LOAD_SYNC_ENABLED)
        self.gpt_load_enabled = bool(self.gpt_load_url and self.gpt_load_auth and self.gpt_load_group_names and self.gpt_load_sync_enabled)

        # 创建线程池用于异步执行
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="SyncUtils")
        self.saving_checkpoint = False

        # 周期性发送控制
        self.batch_interval = 60
        self.batch_timer = None
        self.shutdown_flag = False

        # GPT Load Balancer group ID 缓存 (15分钟缓存)
        self.group_id_cache: Dict[str, int] = {}
        self.group_id_cache_time: Dict[str, float] = {}
        self.group_id_cache_ttl = 15 * 60  # 15分钟

        if not self.balancer_enabled:
            logger.warning("🚫 Gemini Balancer 同步已禁用 - 未配置 URL 或 AUTH")
        else:
            logger.info(f"🔗 Gemini Balancer 已启用 - URL: {self.balancer_url}")

        if not self.gpt_load_enabled:
            logger.warning("🚫 GPT Load Balancer 同步已禁用 - 未配置 URL、AUTH、GROUP_NAME 或同步已禁用")
        else:
            logger.info(f"🔗 GPT Load Balancer 已启用 - URL: {self.gpt_load_url}, 组: {', '.join(self.gpt_load_group_names)}")

        # 启动周期性发送线程
        self._start_batch_sender()

    def add_keys_to_queue(self, keys: List[str]):
        """
        将keys同时添加到balancer和GPT load的发送队列
        
        Args:
            keys: API keys列表
        """
        if not keys:
            return

        # Acquire lock for checkpoint saving
        while self.saving_checkpoint:
            logger.info(f"📥 检查点正在保存中，在将 {len(keys)} 个密钥添加到队列前等待...")
            time.sleep(0.5)  # Small delay to prevent busy-waiting

        self.saving_checkpoint = True  # Acquire the lock
        try:

            # Gemini Balancer
            if self.balancer_enabled:
                initial_balancer_count = len(checkpoint.wait_send_balancer)
                checkpoint.wait_send_balancer.update(keys)
                new_balancer_count = len(checkpoint.wait_send_balancer)
                added_balancer_count = new_balancer_count - initial_balancer_count
                logger.info(f"📥 已将 {added_balancer_count} 个密钥添加到 gemini balancer 队列 (总计: {new_balancer_count})")
            else:
                logger.info(f"🚫 Gemini Balancer 已禁用，跳过 {len(keys)} 个密钥的 gemini balancer 队列")

            # GPT Load Balancer
            if self.gpt_load_enabled:
                initial_gpt_count = len(checkpoint.wait_send_gpt_load)
                checkpoint.wait_send_gpt_load.update(keys)
                new_gpt_count = len(checkpoint.wait_send_gpt_load)
                added_gpt_count = new_gpt_count - initial_gpt_count
                logger.info(f"📥 已将 {added_gpt_count} 个密钥添加到 GPT load balancer 队列 (总计: {new_gpt_count})")
            else:
                logger.info(f"🚫 GPT Load Balancer 已禁用，跳过 {len(keys)} 个密钥的 GPT load balancer 队列")

            file_manager.save_checkpoint(checkpoint)
        finally:
            self.saving_checkpoint = False  # Release the lock

    def _send_balancer_worker(self, keys: List[str]) -> str:
        """
        实际执行发送到balancer的工作函数（在后台线程中执行）
        
        Args:
            keys: API keys列表
            
        Returns:
            str: "ok" if success, otherwise an error code string.
        """
        try:
            logger.info(f"🔄 正在发送 {len(keys)} 个密钥到 balancer...")

            # 1. 获取当前配置
            config_url = f"{self.balancer_url}/api/config"
            headers = {
                'Cookie': f'auth_token={self.balancer_auth}',
                'User-Agent': 'HajimiKing/1.0'
            }

            logger.info(f"📥 正在从以下位置获取当前配置: {config_url}")

            # 获取当前配置
            response = requests.get(config_url, headers=headers, timeout=30)

            if response.status_code != 200:
                logger.error(f"获取配置失败: HTTP {response.status_code} - {response.text}")
                return "get_config_failed_not_200"

            # 解析配置
            config_data = response.json()

            # 2. 获取当前的API_KEYS数组
            current_api_keys = config_data.get('API_KEYS', [])

            # 3. 合并新keys（去重）
            existing_keys_set = set(current_api_keys)
            new_add_keys_set = set()
            for key in keys:
                if key not in existing_keys_set:
                    existing_keys_set.add(key)
                    new_add_keys_set.add(key)

            if len(new_add_keys_set) == 0:
                logger.info(f"ℹ️ 所有 {len(keys)} 个密钥已存在于 balancer 中")
                # 不需要记录发送结果，因为没有实际发送新密钥
                return "ok"

            # 4. 更新配置中的API_KEYS
            config_data['API_KEYS'] = list(existing_keys_set)

            logger.info(f"📝 正在使用 {len(new_add_keys_set)} 个新密钥更新 gemini balancer 配置...")

            # 5. 发送更新后的配置到服务器
            update_headers = headers.copy()
            update_headers['Content-Type'] = 'application/json'

            update_response = requests.put(
                config_url,
                headers=update_headers,
                json=config_data,
                timeout=60
            )

            if update_response.status_code != 200:
                logger.error(f"更新配置失败: HTTP {update_response.status_code} - {update_response.text}")
                return "update_config_failed_not_200"

            # 6. 验证是否添加成功
            updated_config = update_response.json()
            updated_api_keys = updated_config.get('API_KEYS', [])
            updated_keys_set = set(updated_api_keys)

            failed_to_add = [key for key in new_add_keys_set if key not in updated_keys_set]

            if failed_to_add:
                logger.error(f"❌ 添加 {len(failed_to_add)} 个密钥失败: {[key[:10] + '...' for key in failed_to_add]}")
                # 保存发送结果日志 - 部分成功的情况
                send_result = {}
                keys_to_log = []
                for key in new_add_keys_set:  # 只记录尝试新增的密钥
                    if key in failed_to_add:
                        send_result[key] = "update_failed"
                        keys_to_log.append(key)
                    else:
                        send_result[key] = "ok"
                        keys_to_log.append(key)
                if keys_to_log:  # 只有当有需要记录的密钥时才记录
                    file_manager.save_keys_send_result(keys_to_log, send_result)
                return "update_failed"


            logger.info(f"✅ 所有 {len(new_add_keys_set)} 个新密钥已成功添加到 balancer.")
            
            # 保存发送结果日志 - 只记录实际新增的密钥
            send_result = {key: "ok" for key in new_add_keys_set}
            if send_result:  # 只有当有新增密钥时才记录
                file_manager.save_keys_send_result(list(new_add_keys_set), send_result)
            
            return "ok"

        except requests.exceptions.Timeout:
            logger.error("❌ 连接 balancer 时请求超时")
            # 保存发送结果日志 - 所有密钥都失败
            send_result = {key: "timeout" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "timeout"
        except requests.exceptions.ConnectionError:
            logger.error("❌ 连接 balancer 失败")
            # 保存发送结果日志 - 所有密钥都失败
            send_result = {key: "connection_error" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "connection_error"
        except json.JSONDecodeError as e:
            logger.error(f"❌ balancer 返回的 JSON 无效: {str(e)}")
            # 保存发送结果日志 - 所有密钥都失败
            send_result = {key: "json_decode_error" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "json_decode_error"
        except Exception as e:
            logger.error(f"❌ 发送密钥到 balancer 失败: {str(e)}")
            traceback.print_exc()
            # 保存发送结果日志 - 所有密钥都失败
            send_result = {key: "exception" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "exception"

    def _get_gpt_load_group_id(self, group_name: str) -> Optional[int]:
        """
        获取GPT Load Balancer group ID，带缓存功能
        
        Args:
            group_name: 组名
            
        Returns:
            Optional[int]: 组ID，如果未找到则返回None
        """
        current_time = time.time()
        
        # 检查缓存是否有效
        if (group_name in self.group_id_cache and
            group_name in self.group_id_cache_time and
            current_time - self.group_id_cache_time[group_name] < self.group_id_cache_ttl):
            logger.info(f"📋 使用 '{group_name}' 的缓存组 ID: {self.group_id_cache[group_name]}")
            return self.group_id_cache[group_name]
        
        # 缓存过期或不存在，重新获取
        try:
            groups_url = f"{self.gpt_load_url}/api/groups"
            headers = {
                'Authorization': f'Bearer {self.gpt_load_auth}',
                'User-Agent': 'HajimiKing/1.0'
            }

            logger.info(f"📥 正在从以下位置获取组信息: {groups_url}")

            response = requests.get(groups_url, headers=headers, timeout=30)

            if response.status_code != 200:
                logger.error(f"获取组信息失败: HTTP {response.status_code} - {response.text}")
                return None

            groups_data = response.json()
            
            if groups_data.get('code') != 0:
                logger.error(f"组 API 返回错误: {groups_data.get('message', '未知错误')}")
                return None

            # 查找指定group的ID
            groups_list = groups_data.get('data', [])
            for group in groups_list:
                if group.get('name') == group_name:
                    group_id = group.get('id')
                    # 更新缓存
                    self.group_id_cache[group_name] = group_id
                    self.group_id_cache_time[group_name] = current_time
                    logger.info(f"✅ 找到并缓存了组 '{group_name}'，ID: {group_id}")
                    return group_id

            logger.error(f"在组列表中未找到组 '{group_name}'")
            return None

        except Exception as e:
            logger.error(f"❌ 获取 '{group_name}' 的组 ID 失败: {str(e)}")
            return None

    def _send_gpt_load_worker(self, keys: List[str]) -> str:
        """
        实际执行发送到GPT load balancer的工作函数（在后台线程中执行）
        
        Args:
            keys: API keys列表
            
        Returns:
            str: "ok" if success, otherwise an error code string.
        """
        try:
            logger.info(f"🔄 正在发送 {len(keys)} 个密钥到 GPT load balancer，共 {len(self.gpt_load_group_names)} 个组...")

            # 遍历所有group names，为每个group发送keys
            all_success = True
            failed_groups = []
            
            for group_name in self.gpt_load_group_names:
                logger.info(f"📝 正在处理组: {group_name}")
                
                # 1. 获取group ID (使用缓存)
                group_id = self._get_gpt_load_group_id(group_name)
                
                if group_id is None:
                    logger.error(f"获取 '{group_name}' 的组 ID 失败")
                    failed_groups.append(group_name)
                    all_success = False
                    continue

                # 2. 发送keys到指定group
                add_keys_url = f"{self.gpt_load_url}/api/keys/add-async"
                keys_text = ",".join(keys)
                
                add_headers = {
                    'Authorization': f'Bearer {self.gpt_load_auth}',
                    'Content-Type': 'application/json',
                    'User-Agent': 'HajimiKing/1.0'
                }

                payload = {
                    "group_id": group_id,
                    "keys_text": keys_text
                }

                logger.info(f"📝 正在将 {len(keys)} 个密钥添加到组 '{group_name}' (ID: {group_id})...")

                try:
                    # 发送添加keys请求
                    add_response = requests.post(
                        add_keys_url,
                        headers=add_headers,
                        json=payload,
                        timeout=60
                    )

                    if add_response.status_code != 200:
                        logger.error(f"添加密钥到组 '{group_name}' 失败: HTTP {add_response.status_code} - {add_response.text}")
                        failed_groups.append(group_name)
                        all_success = False
                        continue

                    # 解析添加keys响应
                    add_data = add_response.json()
                    
                    if add_data.get('code') != 0:
                        logger.error(f"添加密钥 API 对组 '{group_name}' 返回错误: {add_data.get('message', '未知错误')}")
                        failed_groups.append(group_name)
                        all_success = False
                        continue

                    # 检查任务状态
                    task_data = add_data.get('data', {})
                    task_type = task_data.get('task_type')
                    is_running = task_data.get('is_running')
                    total = task_data.get('total', 0)
                    response_group_name = task_data.get('group_name')

                    logger.info(f"✅ 组 '{group_name}' 的密钥添加任务已成功启动:")
                    logger.info(f"   任务类型: {task_type}")
                    logger.info(f"   是否运行: {is_running}")
                    logger.info(f"   总密钥数: {total}")
                    logger.info(f"   组名: {response_group_name}")

                except Exception as e:
                    logger.error(f"❌ 向组 '{group_name}' 添加密钥时出现异常: {str(e)}")
                    failed_groups.append(group_name)
                    all_success = False
                    continue

            # 根据结果返回状态
            if all_success:
                logger.info(f"✅ 已成功将密钥发送到所有 {len(self.gpt_load_group_names)} 个组")
                # 保存发送结果日志 - 所有密钥都成功
                send_result = {key: "ok" for key in keys}
                file_manager.save_keys_send_result(keys, send_result)
                return "ok"
            else:
                logger.error(f"❌ 发送密钥到 {len(failed_groups)} 个组失败: {', '.join(failed_groups)}")
                # 保存发送结果日志 - 部分或全部失败
                send_result = {key: f"partial_failure_{len(failed_groups)}_groups" for key in keys}
                file_manager.save_keys_send_result(keys, send_result)
                return "partial_failure"

        except requests.exceptions.Timeout:
            logger.error("❌ 连接 GPT load balancer 时请求超时")
            send_result = {key: "timeout" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "timeout"
        except requests.exceptions.ConnectionError:
            logger.error("❌ 连接 GPT load balancer 失败")
            send_result = {key: "connection_error" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "connection_error"
        except json.JSONDecodeError as e:
            logger.error(f"❌ GPT load balancer 返回的 JSON 无效: {str(e)}")
            send_result = {key: "json_decode_error" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "json_decode_error"
        except Exception as e:
            logger.error(f"❌ 发送密钥到 GPT load balancer 失败: {str(e)}", exc_info=True)
            send_result = {key: "exception" for key in keys}
            file_manager.save_keys_send_result(keys, send_result)
            return "exception"

    def _start_batch_sender(self) -> None:
        """启动批量发送定时器"""
        if self.shutdown_flag:
            return

        # 启动发送任务
        self.executor.submit(self._batch_send_worker)

        # 设置下一次发送定时器
        self.batch_timer = threading.Timer(self.batch_interval, self._start_batch_sender)
        self.batch_timer.daemon = True
        self.batch_timer.start()

    def _batch_send_worker(self) -> None:
        """批量发送worker"""
        while self.saving_checkpoint:
            logger.info(f"📥 [Sync] 检查点正在保存中，在批量发送前等待...")
            time.sleep(0.5)

        self.saving_checkpoint = True
        try:
            # 加载checkpoint
            logger.info(f"📥 [Sync] 开始批量发送，wait_send_balancer 长度: {len(checkpoint.wait_send_balancer)}, wait_send_gpt_load 长度: {len(checkpoint.wait_send_gpt_load)}")
            # 发送gemini balancer队列
            if checkpoint.wait_send_balancer and self.balancer_enabled:
                balancer_keys = list(checkpoint.wait_send_balancer)
                logger.info(f"🔄 [Sync] 正在处理 gemini balancer 队列中的 {len(balancer_keys)} 个密钥")

                result_code = self._send_balancer_worker(balancer_keys)
                if result_code == 'ok':
                    # 清空队列
                    checkpoint.wait_send_balancer.clear()
                    logger.info(f"✅ Gemini balancer 队列处理成功，已清除 {len(balancer_keys)} 个密钥")
                else:
                    logger.error(f"❌ Gemini balancer 队列处理失败，错误代码: {result_code}")

            # 发送gpt_load队列
            if checkpoint.wait_send_gpt_load and self.gpt_load_enabled:
                gpt_load_keys = list(checkpoint.wait_send_gpt_load)
                logger.info(f"🔄 正在处理 GPT load balancer 队列中的 {len(gpt_load_keys)} 个密钥")

                result_code = self._send_gpt_load_worker(gpt_load_keys)

                if result_code == 'ok':
                    # 清空队列
                    checkpoint.wait_send_gpt_load.clear()
                    logger.info(f"✅ GPT load balancer 队列处理成功，已清除 {len(gpt_load_keys)} 个密钥")
                else:
                    logger.error(f"❌ GPT load balancer 队列处理失败，错误代码: {result_code}")

            # 保存checkpoint
            file_manager.save_checkpoint(checkpoint)
        except Exception as e:
            stacktrace = traceback.format_exc()
            logger.error(f"❌ 批量发送工作器错误: {e}\n{stacktrace}")
            logger.error(f"❌ 批量发送工作器错误: {e}")
        finally:
            self.saving_checkpoint = False  # Release the lock

    def shutdown(self) -> None:
        """关闭线程池和定时器"""
        self.shutdown_flag = True

        if self.batch_timer:
            self.batch_timer.cancel()

        self.executor.shutdown(wait=True)
        logger.info("🔚 SyncUtils 关闭完成")


# 创建全局实例
sync_utils = SyncUtils()
