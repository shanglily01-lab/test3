"""
认证服务模块
提供JWT令牌生成/验证、密码加密/验证等功能
"""

import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import bcrypt
import jwt
import pymysql
from loguru import logger


class AuthService:
    """认证服务类"""

    def __init__(self, db_config: Dict, jwt_config: Dict):
        """
        初始化认证服务

        Args:
            db_config: 数据库配置
            jwt_config: JWT配置，包含:
                - secret_key: JWT密钥
                - algorithm: 算法 (默认 HS256)
                - access_token_expire_minutes: 访问令牌过期时间 (默认 15分钟)
                - refresh_token_expire_days: 刷新令牌过期时间 (默认 30天)
        """
        self.db_config = db_config
        self.secret_key = jwt_config.get('secret_key', 'your-secret-key-change-in-production')
        self.algorithm = jwt_config.get('algorithm', 'HS256')
        self.access_token_expire_minutes = jwt_config.get('access_token_expire_minutes', 1440)  # 默认24小时
        self.refresh_token_expire_days = jwt_config.get('refresh_token_expire_days', 30)

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

    # ==================== 密码处理 ====================

    def hash_password(self, password: str) -> str:
        """
        对密码进行哈希

        Args:
            password: 明文密码

        Returns:
            哈希后的密码
        """
        salt = bcrypt.gensalt(rounds=12)
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    def verify_password(self, password: str, password_hash: str) -> bool:
        """
        验证密码

        Args:
            password: 明文密码
            password_hash: 哈希后的密码

        Returns:
            密码是否匹配
        """
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except Exception as e:
            logger.error(f"密码验证失败: {e}")
            return False

    # ==================== JWT令牌处理 ====================

    def create_access_token(self, user_id: int, username: str, role: str) -> str:
        """
        创建访问令牌

        Args:
            user_id: 用户ID
            username: 用户名
            role: 用户角色

        Returns:
            JWT访问令牌
        """
        expire = datetime.now() + timedelta(minutes=self.access_token_expire_minutes)
        payload = {
            'sub': str(user_id),
            'username': username,
            'role': role,
            'type': 'access',
            'exp': expire,
            'iat': datetime.now()
        }
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)

    def create_refresh_token(self, user_id: int, device_info: str = None, ip_address: str = None) -> Tuple[str, datetime]:
        """
        创建刷新令牌并存储到数据库

        Args:
            user_id: 用户ID
            device_info: 设备信息
            ip_address: IP地址

        Returns:
            (刷新令牌, 过期时间)
        """
        # 生成随机令牌
        token = secrets.token_urlsafe(64)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        expires_at = datetime.now() + timedelta(days=self.refresh_token_expire_days)

        # 存储到数据库
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO refresh_tokens (user_id, token_hash, device_info, ip_address, expires_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, token_hash, device_info, ip_address, expires_at))
                conn.commit()
        finally:
            conn.close()

        return token, expires_at

    def verify_access_token(self, token: str) -> Optional[Dict]:
        """
        验证访问令牌

        Args:
            token: JWT访问令牌

        Returns:
            解码后的payload，验证失败返回None
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            if payload.get('type') != 'access':
                return None
            return payload
        except jwt.ExpiredSignatureError:
            logger.debug("访问令牌已过期")
            return None
        except jwt.InvalidTokenError as e:
            logger.debug(f"无效的访问令牌: {e}")
            return None

    def verify_refresh_token(self, token: str) -> Optional[Dict]:
        """
        验证刷新令牌

        Args:
            token: 刷新令牌

        Returns:
            用户信息，验证失败返回None
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT rt.id, rt.user_id, rt.expires_at, rt.revoked_at,
                           u.username, u.role, u.status
                    FROM refresh_tokens rt
                    JOIN users u ON rt.user_id = u.id
                    WHERE rt.token_hash = %s
                """, (token_hash,))
                result = cursor.fetchone()

                if not result:
                    return None

                # 检查是否已撤销
                if result['revoked_at']:
                    return None

                # 检查是否过期
                if result['expires_at'] < datetime.now():
                    return None

                # 检查用户状态
                if result['status'] != 'active':
                    return None

                return {
                    'user_id': result['user_id'],
                    'username': result['username'],
                    'role': result['role'],
                    'token_id': result['id']
                }
        finally:
            conn.close()

    def revoke_refresh_token(self, token: str) -> bool:
        """
        撤销刷新令牌

        Args:
            token: 刷新令牌

        Returns:
            是否成功
        """
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE refresh_tokens
                    SET revoked_at = NOW()
                    WHERE token_hash = %s AND revoked_at IS NULL
                """, (token_hash,))
                conn.commit()
                return cursor.rowcount > 0
        finally:
            conn.close()

    def revoke_all_user_tokens(self, user_id: int) -> int:
        """
        撤销用户的所有刷新令牌

        Args:
            user_id: 用户ID

        Returns:
            撤销的令牌数量
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE refresh_tokens
                    SET revoked_at = NOW()
                    WHERE user_id = %s AND revoked_at IS NULL
                """, (user_id,))
                conn.commit()
                return cursor.rowcount
        finally:
            conn.close()

    # ==================== 用户管理 ====================

    def register_user(self, username: str, email: str, password: str, role: str = 'user') -> Dict:
        """
        注册新用户

        Args:
            username: 用户名
            email: 邮箱
            password: 密码
            role: 角色 (默认 user)

        Returns:
            {'success': True, 'user_id': id} 或 {'success': False, 'error': msg}
        """
        # 验证输入
        if len(username) < 3 or len(username) > 50:
            return {'success': False, 'error': '用户名长度应为3-50个字符'}
        if len(password) < 6:
            return {'success': False, 'error': '密码长度至少6个字符'}
        if '@' not in email:
            return {'success': False, 'error': '邮箱格式不正确'}

        password_hash = self.hash_password(password)

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # 检查用户名是否存在
                cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
                if cursor.fetchone():
                    return {'success': False, 'error': '用户名已存在'}

                # 检查邮箱是否存在
                cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
                if cursor.fetchone():
                    return {'success': False, 'error': '邮箱已被注册'}

                # 插入新用户
                cursor.execute("""
                    INSERT INTO users (username, email, password_hash, role, status)
                    VALUES (%s, %s, %s, %s, 'active')
                """, (username, email, password_hash, role))
                conn.commit()

                user_id = cursor.lastrowid
                logger.info(f"新用户注册成功: {username} (ID: {user_id})")

                return {'success': True, 'user_id': user_id}
        except Exception as e:
            logger.error(f"注册失败: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()

    def authenticate_user(self, username: str, password: str, ip_address: str = None, user_agent: str = None) -> Dict:
        """
        验证用户登录

        Args:
            username: 用户名或邮箱
            password: 密码
            ip_address: IP地址
            user_agent: 浏览器UA

        Returns:
            {'success': True, 'user': {...}} 或 {'success': False, 'error': msg}
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                # 支持用户名或邮箱登录
                cursor.execute("""
                    SELECT id, username, email, password_hash, role, status
                    FROM users
                    WHERE username = %s OR email = %s
                """, (username, username))
                user = cursor.fetchone()

                # 记录登录日志
                def log_login(success: bool, user_id: int = None, failure_reason: str = None):
                    try:
                        cursor.execute("""
                            INSERT INTO login_logs (user_id, username, success, ip_address, user_agent, failure_reason)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (user_id, username, success, ip_address, user_agent, failure_reason))
                        conn.commit()
                    except Exception as e:
                        logger.error(f"记录登录日志失败: {e}")

                if not user:
                    log_login(False, failure_reason='用户不存在')
                    return {'success': False, 'error': '用户名或密码错误'}

                if user['status'] != 'active':
                    log_login(False, user['id'], f"账户状态: {user['status']}")
                    return {'success': False, 'error': '账户已被禁用'}

                if not self.verify_password(password, user['password_hash']):
                    log_login(False, user['id'], '密码错误')
                    return {'success': False, 'error': '用户名或密码错误'}

                # 更新最后登录时间
                cursor.execute("""
                    UPDATE users SET last_login = NOW() WHERE id = %s
                """, (user['id'],))
                conn.commit()

                log_login(True, user['id'])
                logger.info(f"用户登录成功: {user['username']} (ID: {user['id']})")

                return {
                    'success': True,
                    'user': {
                        'id': user['id'],
                        'username': user['username'],
                        'email': user['email'],
                        'role': user['role']
                    }
                }
        finally:
            conn.close()

    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """
        根据ID获取用户信息

        Args:
            user_id: 用户ID

        Returns:
            用户信息或None
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, username, email, role, status, last_login, created_at
                    FROM users WHERE id = %s
                """, (user_id,))
                return cursor.fetchone()
        finally:
            conn.close()

    def change_password(self, user_id: int, old_password: str, new_password: str) -> Dict:
        """
        修改密码

        Args:
            user_id: 用户ID
            old_password: 旧密码
            new_password: 新密码

        Returns:
            {'success': True} 或 {'success': False, 'error': msg}
        """
        if len(new_password) < 6:
            return {'success': False, 'error': '新密码长度至少6个字符'}

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT password_hash FROM users WHERE id = %s", (user_id,))
                user = cursor.fetchone()

                if not user:
                    return {'success': False, 'error': '用户不存在'}

                if not self.verify_password(old_password, user['password_hash']):
                    return {'success': False, 'error': '原密码错误'}

                new_hash = self.hash_password(new_password)
                cursor.execute("""
                    UPDATE users SET password_hash = %s WHERE id = %s
                """, (new_hash, user_id))
                conn.commit()

                # 撤销所有刷新令牌，强制重新登录
                self.revoke_all_user_tokens(user_id)

                logger.info(f"用户 {user_id} 修改密码成功")
                return {'success': True}
        finally:
            conn.close()


# 全局实例
_auth_service: Optional[AuthService] = None


def init_auth_service(db_config: Dict, jwt_config: Dict) -> AuthService:
    """初始化全局认证服务"""
    global _auth_service
    _auth_service = AuthService(db_config, jwt_config)
    return _auth_service


def get_auth_service() -> AuthService:
    """获取全局认证服务实例"""
    if _auth_service is None:
        raise RuntimeError("AuthService未初始化，请先调用init_auth_service()")
    return _auth_service
