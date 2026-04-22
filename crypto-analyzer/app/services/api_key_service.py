# -*- coding: utf-8 -*-
"""
API密钥管理服务
负责用户API密钥的加密存储、解密读取
"""

import os
import base64
from typing import Dict, Optional, List
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import pymysql
from loguru import logger


class APIKeyService:
    """API密钥管理服务"""

    def __init__(self, db_config: Dict, encryption_key: str = None):
        """
        初始化API密钥服务

        Args:
            db_config: 数据库配置
            encryption_key: 加密密钥（从环境变量或配置获取）
        """
        self.db_config = db_config

        # 获取加密密钥
        if encryption_key is None:
            encryption_key = os.environ.get('API_KEY_ENCRYPTION_KEY', '')

        if not encryption_key:
            # 如果没有配置，使用 JWT_SECRET_KEY 派生
            jwt_secret = os.environ.get('JWT_SECRET_KEY', 'default-secret-key')
            encryption_key = jwt_secret

        # 从密钥派生 Fernet 密钥
        self.fernet = self._create_fernet(encryption_key)

    def _create_fernet(self, password: str) -> Fernet:
        """从密码创建 Fernet 加密器"""
        # 使用固定 salt（生产环境应该每个用户不同）
        salt = b'crypto-analyzer-salt-v1'
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return Fernet(key)

    def _encrypt(self, plaintext: str) -> str:
        """加密字符串"""
        if not plaintext:
            return ''
        return self.fernet.encrypt(plaintext.encode()).decode()

    def _decrypt(self, ciphertext: str) -> str:
        """解密字符串"""
        if not ciphertext:
            return ''
        try:
            return self.fernet.decrypt(ciphertext.encode()).decode()
        except Exception as e:
            logger.error(f"解密失败: {e}")
            return ''

    def _get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(
            host=self.db_config.get('host', 'localhost'),
            port=self.db_config.get('port', 3306),
            user=self.db_config.get('user', 'root'),
            password=self.db_config.get('password', ''),
            database=self.db_config.get('database', 'binance-data'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def save_api_key(
        self,
        user_id: int,
        exchange: str,
        account_name: str,
        api_key: str,
        api_secret: str,
        permissions: str = 'spot,futures',
        is_testnet: bool = False,
        max_position_value: float = 1000.0,
        max_daily_loss: float = 100.0,
        max_leverage: int = 10,
        margin_per_trade: float = 40.0,
    ) -> Dict:
        """
        保存用户API密钥

        Args:
            user_id: 用户ID
            exchange: 交易所名称
            account_name: 账户名称
            api_key: API Key
            api_secret: API Secret
            permissions: 权限
            is_testnet: 是否测试网
            max_position_value: 最大持仓价值
            max_daily_loss: 最大日亏损
            max_leverage: 最大杠杆

        Returns:
            {'success': True, 'api_key_id': id} 或 {'success': False, 'error': msg}
        """
        try:
            # 加密 API 密钥
            encrypted_key = self._encrypt(api_key)
            encrypted_secret = self._encrypt(api_secret)

            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    # 检查是否已存在
                    cursor.execute(
                        """SELECT id FROM user_api_keys
                        WHERE user_id = %s AND exchange = %s AND account_name = %s""",
                        (user_id, exchange, account_name)
                    )
                    existing = cursor.fetchone()

                    if existing:
                        # 更新
                        cursor.execute(
                            """UPDATE user_api_keys SET
                                api_key = %s,
                                api_secret = %s,
                                permissions = %s,
                                is_testnet = %s,
                                max_position_value = %s,
                                max_daily_loss = %s,
                                max_leverage = %s,
                                margin_per_trade = %s,
                                status = 'active',
                                updated_at = NOW()
                            WHERE id = %s""",
                            (encrypted_key, encrypted_secret, permissions, is_testnet,
                             max_position_value, max_daily_loss, max_leverage,
                             margin_per_trade, existing['id'])
                        )
                        api_key_id = existing['id']
                    else:
                        # 插入
                        cursor.execute(
                            """INSERT INTO user_api_keys
                            (user_id, exchange, account_name, api_key, api_secret,
                             permissions, is_testnet, max_position_value, max_daily_loss,
                             max_leverage, margin_per_trade)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (user_id, exchange, account_name, encrypted_key, encrypted_secret,
                             permissions, is_testnet, max_position_value, max_daily_loss,
                             max_leverage, margin_per_trade)
                        )
                        api_key_id = cursor.lastrowid

                conn.commit()
                logger.info(f"用户 {user_id} 的 {exchange} API密钥已保存")
                return {'success': True, 'api_key_id': api_key_id}

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"保存API密钥失败: {e}")
            return {'success': False, 'error': str(e)}

    def get_api_key(self, user_id: int, exchange: str = 'binance', account_name: str = None) -> Optional[Dict]:
        """
        获取用户API密钥（解密后）

        Args:
            user_id: 用户ID
            exchange: 交易所名称
            account_name: 账户名称（可选，不指定则返回默认的）

        Returns:
            {'api_key': '...', 'api_secret': '...', ...} 或 None
        """
        try:
            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    if account_name:
                        cursor.execute(
                            """SELECT * FROM user_api_keys
                            WHERE user_id = %s AND exchange = %s AND account_name = %s AND status = 'active'""",
                            (user_id, exchange, account_name)
                        )
                    else:
                        # 返回第一个活跃的
                        cursor.execute(
                            """SELECT * FROM user_api_keys
                            WHERE user_id = %s AND exchange = %s AND status = 'active'
                            ORDER BY created_at ASC LIMIT 1""",
                            (user_id, exchange)
                        )

                    row = cursor.fetchone()
                    if not row:
                        return None

                    # 解密
                    return {
                        'id': row['id'],
                        'user_id': row['user_id'],
                        'exchange': row['exchange'],
                        'account_name': row['account_name'],
                        'api_key': self._decrypt(row['api_key']),
                        'api_secret': self._decrypt(row['api_secret']),
                        'permissions': row['permissions'],
                        'is_testnet': row['is_testnet'],
                        'max_position_value': float(row['max_position_value']) if row['max_position_value'] else 1000.0,
                        'max_daily_loss': float(row['max_daily_loss']) if row['max_daily_loss'] else 100.0,
                        'max_leverage': row['max_leverage'] or 10,
                        'margin_per_trade': float(row['margin_per_trade']) if row.get('margin_per_trade') else 40.0,
                        'status': row['status']
                    }

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"获取API密钥失败: {e}")
            return None

    def get_user_api_keys(self, user_id: int) -> List[Dict]:
        """
        获取用户所有API密钥（不含密钥内容，只含元信息）

        Args:
            user_id: 用户ID

        Returns:
            API密钥列表
        """
        try:
            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """SELECT id, exchange, account_name, permissions, is_testnet,
                            max_position_value, max_daily_loss, max_leverage, margin_per_trade,
                            status, last_used_at, created_at
                        FROM user_api_keys
                        WHERE user_id = %s
                        ORDER BY exchange, account_name""",
                        (user_id,)
                    )
                    rows = cursor.fetchall()

                    result = []
                    for row in rows:
                        result.append({
                            'id': row['id'],
                            'exchange': row['exchange'],
                            'account_name': row['account_name'],
                            'permissions': row['permissions'],
                            'is_testnet': bool(row['is_testnet']),
                            'max_position_value': float(row['max_position_value']) if row['max_position_value'] else None,
                            'max_daily_loss': float(row['max_daily_loss']) if row['max_daily_loss'] else None,
                            'max_leverage': row['max_leverage'],
                            'margin_per_trade': float(row['margin_per_trade']) if row.get('margin_per_trade') else 40.0,
                            'status': row['status'],
                            'last_used_at': row['last_used_at'].isoformat() if row['last_used_at'] else None,
                            'created_at': row['created_at'].isoformat() if row['created_at'] else None
                        })

                    return result

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"获取用户API密钥列表失败: {e}")
            return []

    def get_all_active_api_keys(self, exchange: str = 'binance') -> List[Dict]:
        """
        获取所有用户的全部激活API密钥（含解密后的密钥），用于实盘同步开仓。

        Returns:
            [{'id', 'user_id', 'account_name', 'api_key', 'api_secret',
              'max_position_value', 'max_leverage', 'max_daily_loss'}, ...]
        """
        try:
            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """SELECT id, user_id, account_name, api_key, api_secret,
                            max_position_value, max_leverage, max_daily_loss
                        FROM user_api_keys
                        WHERE exchange = %s AND status = 'active'
                        ORDER BY id""",
                        (exchange,)
                    )
                    rows = cursor.fetchall()
                    result = []
                    for row in rows:
                        try:
                            result.append({
                                'id': row['id'],
                                'user_id': row['user_id'],
                                'account_name': row['account_name'],
                                'api_key': self._decrypt(row['api_key']),
                                'api_secret': self._decrypt(row['api_secret']),
                                'max_position_value': float(row['max_position_value']) if row['max_position_value'] else 100.0,
                                'max_leverage': int(row['max_leverage']) if row['max_leverage'] else 5,
                                'max_daily_loss': float(row['max_daily_loss']) if row['max_daily_loss'] else 50.0,
                            })
                        except Exception as dec_err:
                            logger.warning(f"解密API密钥失败(id={row['id']}): {dec_err}")
                    return result
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"获取全部激活API密钥失败: {e}")
            return []

    def delete_api_key(self, user_id: int, api_key_id: int) -> Dict:
        """
        删除API密钥

        Args:
            user_id: 用户ID
            api_key_id: API密钥ID

        Returns:
            {'success': True} 或 {'success': False, 'error': msg}
        """
        try:
            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    # 确保只能删除自己的
                    cursor.execute(
                        """DELETE FROM user_api_keys WHERE id = %s AND user_id = %s""",
                        (api_key_id, user_id)
                    )
                    if cursor.rowcount == 0:
                        return {'success': False, 'error': 'API密钥不存在或无权限'}

                conn.commit()
                logger.info(f"用户 {user_id} 删除了 API密钥 {api_key_id}")
                return {'success': True}

            finally:
                conn.close()

        except Exception as e:
            logger.error(f"删除API密钥失败: {e}")
            return {'success': False, 'error': str(e)}

    def get_api_key_by_id(self, user_id: int, api_key_id: int) -> Optional[Dict]:
        """
        通过ID获取API密钥（解密后），同时验证归属关系

        Args:
            user_id: 用户ID（用于权限验证）
            api_key_id: user_api_keys.id

        Returns:
            解密后的密钥字典，或 None
        """
        try:
            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """SELECT * FROM user_api_keys
                        WHERE id = %s AND user_id = %s AND status = 'active'""",
                        (api_key_id, user_id)
                    )
                    row = cursor.fetchone()
                    if not row:
                        return None
                    return {
                        'id': row['id'],
                        'user_id': row['user_id'],
                        'exchange': row['exchange'],
                        'account_name': row['account_name'],
                        'api_key': self._decrypt(row['api_key']),
                        'api_secret': self._decrypt(row['api_secret']),
                        'permissions': row['permissions'],
                        'is_testnet': bool(row['is_testnet']),
                        'status': row['status']
                    }
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"通过ID获取API密钥失败: {e}")
            return None

    def update_last_used(self, api_key_id: int):
        """更新最后使用时间"""
        try:
            conn = self._get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE user_api_keys SET last_used_at = NOW() WHERE id = %s",
                        (api_key_id,)
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"更新API密钥最后使用时间失败: {e}")

    def verify_api_key(self, user_id: int, exchange: str = 'binance') -> Dict:
        """
        验证用户的API密钥是否有效

        Args:
            user_id: 用户ID
            exchange: 交易所

        Returns:
            {'success': True, 'balance': {...}} 或 {'success': False, 'error': msg}
        """
        api_keys = self.get_api_key(user_id, exchange)
        if not api_keys:
            return {'success': False, 'error': '未配置API密钥'}

        try:
            if exchange == 'binance':
                from app.trading.binance_futures_engine import BinanceFuturesEngine

                # 创建临时引擎验证
                temp_engine = BinanceFuturesEngine(
                    self.db_config,
                    api_key=api_keys['api_key'],
                    api_secret=api_keys['api_secret']
                )
                balance = temp_engine.get_account_balance()

                if balance:
                    self.update_last_used(api_keys['id'])
                    return {'success': True, 'balance': balance}
                else:
                    return {'success': False, 'error': '无法获取账户信息，请检查API权限'}

            else:
                return {'success': False, 'error': f'暂不支持 {exchange} 交易所'}

        except Exception as e:
            logger.error(f"验证API密钥失败: {e}")
            return {'success': False, 'error': str(e)}


# 全局实例
_api_key_service: Optional[APIKeyService] = None


def get_api_key_service() -> Optional[APIKeyService]:
    """获取API密钥服务实例"""
    return _api_key_service


def init_api_key_service(db_config: Dict) -> APIKeyService:
    """初始化API密钥服务"""
    global _api_key_service
    _api_key_service = APIKeyService(db_config)
    return _api_key_service
