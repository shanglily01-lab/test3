/**
 * 用户认证工具库
 * 提供登录状态管理、token刷新、认证请求等功能
 */

const Auth = {
    // ==================== 存储键名 ====================
    KEYS: {
        ACCESS_TOKEN: 'access_token',
        REFRESH_TOKEN: 'refresh_token',
        USER: 'user',
        REMEMBER_ME: 'remember_me'
    },

    // ==================== 基础方法 ====================

    /**
     * 获取访问令牌
     */
    getAccessToken() {
        return localStorage.getItem(this.KEYS.ACCESS_TOKEN);
    },

    /**
     * 获取刷新令牌
     */
    getRefreshToken() {
        return localStorage.getItem(this.KEYS.REFRESH_TOKEN);
    },

    /**
     * 获取当前用户信息
     */
    getUser() {
        const user = localStorage.getItem(this.KEYS.USER);
        if (user) {
            try {
                return JSON.parse(user);
            } catch (e) {
                return null;
            }
        }
        return null;
    },

    /**
     * 检查是否已登录
     */
    isLoggedIn() {
        return !!this.getAccessToken();
    },

    /**
     * 保存登录信息
     */
    saveLoginInfo(accessToken, refreshToken, user) {
        localStorage.setItem(this.KEYS.ACCESS_TOKEN, accessToken);
        localStorage.setItem(this.KEYS.REFRESH_TOKEN, refreshToken);
        localStorage.setItem(this.KEYS.USER, JSON.stringify(user));
    },

    /**
     * 清除登录信息
     */
    clearLoginInfo() {
        localStorage.removeItem(this.KEYS.ACCESS_TOKEN);
        localStorage.removeItem(this.KEYS.REFRESH_TOKEN);
        localStorage.removeItem(this.KEYS.USER);
        localStorage.removeItem(this.KEYS.REMEMBER_ME);
    },

    // ==================== API方法 ====================

    /**
     * 登录
     */
    async login(username, password) {
        const response = await fetch('/api/auth/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username, password })
        });

        const data = await response.json();

        if (response.ok && data.success) {
            this.saveLoginInfo(data.access_token, data.refresh_token, data.user);
            return { success: true, user: data.user };
        } else {
            return { success: false, error: data.detail || data.error || '登录失败' };
        }
    },

    /**
     * 注册
     */
    async register(username, email, password) {
        const response = await fetch('/api/auth/register', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ username, email, password })
        });

        const data = await response.json();

        if (response.ok && data.success) {
            return { success: true, user_id: data.user_id };
        } else {
            return { success: false, error: data.detail || data.error || '注册失败' };
        }
    },

    /**
     * 退出登录
     */
    async logout() {
        const refreshToken = this.getRefreshToken();

        if (refreshToken) {
            try {
                await fetch('/api/auth/logout', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ refresh_token: refreshToken })
                });
            } catch (error) {
                console.error('退出登录请求失败:', error);
            }
        }

        this.clearLoginInfo();
    },

    /**
     * 验证token是否有效
     */
    async verifyToken() {
        const token = this.getAccessToken();
        if (!token) return false;

        try {
            const response = await fetch('/api/auth/verify', {
                headers: {
                    'Authorization': `Bearer ${token}`
                }
            });

            if (response.ok) {
                return true;
            }

            // token无效，尝试刷新
            return await this.refreshToken();
        } catch (error) {
            console.error('验证token失败:', error);
            return false;
        }
    },

    /**
     * 刷新访问令牌
     */
    async refreshToken() {
        const refreshToken = this.getRefreshToken();
        if (!refreshToken) return false;

        try {
            const response = await fetch('/api/auth/refresh', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ refresh_token: refreshToken })
            });

            if (response.ok) {
                const data = await response.json();
                localStorage.setItem(this.KEYS.ACCESS_TOKEN, data.access_token);
                return true;
            } else {
                this.clearLoginInfo();
                return false;
            }
        } catch (error) {
            console.error('刷新token失败:', error);
            return false;
        }
    },

    /**
     * 获取当前用户详细信息（从服务器）
     */
    async fetchUserInfo() {
        const token = this.getAccessToken();
        if (!token) return null;

        try {
            const response = await fetch('/api/auth/me', {
                headers: {
                    'Authorization': `Bearer ${token}`
                }
            });

            if (response.ok) {
                const data = await response.json();
                return data.user;
            }
            return null;
        } catch (error) {
            console.error('获取用户信息失败:', error);
            return null;
        }
    },

    // ==================== 请求辅助方法 ====================

    /**
     * 获取认证请求头
     */
    getAuthHeaders() {
        const token = this.getAccessToken();
        if (token) {
            return {
                'Authorization': `Bearer ${token}`
            };
        }
        return {};
    },

    /**
     * 发送带认证的请求
     */
    async authFetch(url, options = {}) {
        const headers = {
            ...options.headers,
            ...this.getAuthHeaders()
        };

        let response = await fetch(url, { ...options, headers });

        // 如果401，尝试刷新token后重试
        if (response.status === 401) {
            const refreshed = await this.refreshToken();
            if (refreshed) {
                headers['Authorization'] = `Bearer ${this.getAccessToken()}`;
                response = await fetch(url, { ...options, headers });
            }
        }

        return response;
    },

    // ==================== 页面保护 ====================

    /**
     * 要求登录（未登录则跳转到登录页）
     */
    requireLogin(redirectUrl = null) {
        if (!this.isLoggedIn()) {
            const currentUrl = redirectUrl || window.location.pathname + window.location.search;
            window.location.href = `/login?redirect=${encodeURIComponent(currentUrl)}`;
            return false;
        }
        return true;
    },

    /**
     * 要求管理员权限
     */
    requireAdmin() {
        if (!this.requireLogin()) return false;

        const user = this.getUser();
        if (user && user.role === 'admin') {
            return true;
        }

        if (typeof window !== 'undefined' && typeof window.showToast === 'function') {
            window.showToast('需要管理员权限', 'error');
        } else {
            alert('需要管理员权限');
        }
        return false;
    }
};

// 导出（如果支持ES模块）
if (typeof module !== 'undefined' && module.exports) {
    module.exports = Auth;
}
