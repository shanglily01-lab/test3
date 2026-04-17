/**
 * 统一的时间格式化工具
 *
 * 规则：
 * - 后端和数据库：本地时间（UTC+8）
 * - 前端显示：直接使用，无需转换
 * - 显示格式：本地时间，可选标注 UTC+8
 */

/**
 * 格式化本地时间字符串（DB 存储本地时间，无需 UTC 转换）
 * @param {string|Date} utcTime - 本地时间字符串或Date对象
 * @param {string} format - 格式类型: 'full'（完整）, 'datetime'（日期+时间）, 'date'（仅日期）, 'time'（仅时间）, 'relative'（相对时间）
 * @param {boolean} showTimezone - 是否显示时区标识（默认true）
 * @returns {string} 格式化后的时间字符串
 */
function formatTimeUTC8(utcTime, format = 'datetime', showTimezone = true) {
    if (!utcTime) return '-';

    try {
        // 统一转换为Date对象
        let date;
        if (utcTime instanceof Date) {
            date = utcTime;
        } else if (typeof utcTime === 'string') {
            // DB 存储本地时间，去除时区后缀直接解析为本地时间
            const s = utcTime.replace(' ', 'T').replace(/Z$/, '').replace(/[+-]\d{2}:\d{2}$/, '');
            date = new Date(s);
        } else {
            return '-';
        }

        // 检查日期是否有效
        if (isNaN(date.getTime())) {
            return '-';
        }

        // 直接使用本地时间，无需偏移
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        const seconds = String(date.getSeconds()).padStart(2, '0');

        const tz = showTimezone ? ' (UTC+8)' : '';

        switch (format) {
            case 'full':
                // 完整格式: 2026-01-23 15:30:45 (UTC+8)
                return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}${tz}`;

            case 'datetime':
                // 日期+时间: 01-23 15:30 (UTC+8)
                return `${month}-${day} ${hours}:${minutes}${tz}`;

            case 'date':
                // 仅日期: 2026-01-23
                return `${year}-${month}-${day}`;

            case 'time':
                // 仅时间: 15:30:45
                return `${hours}:${minutes}:${seconds}`;

            case 'relative':
                // 相对时间: 5分钟前, 2小时前
                return formatRelativeTime(date, showTimezone);

            default:
                return `${month}-${day} ${hours}:${minutes}${tz}`;
        }
    } catch (e) {
        console.error('时间格式化失败:', e, utcTime);
        return '-';
    }
}

/**
 * 格式化相对时间（例如：5分钟前）
 * @param {Date} date - 时间对象
 * @param {boolean} showTimezone - 是否显示时区标识
 * @returns {string} 相对时间字符串
 */
function formatRelativeTime(date, showTimezone = true) {
    const now = new Date();
    const diffMs = now - date;
    const diffMinutes = Math.floor(diffMs / 1000 / 60);
    const diffHours = Math.floor(diffMinutes / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMinutes < 1) {
        return '刚刚';
    } else if (diffMinutes < 60) {
        return `${diffMinutes}分钟前`;
    } else if (diffHours < 24) {
        return `${diffHours}小时前`;
    } else if (diffDays < 7) {
        return `${diffDays}天前`;
    } else {
        // 超过7天，显示具体日期
        return formatTimeUTC8(date, 'datetime', showTimezone);
    }
}

/**
 * 格式化时间范围
 * @param {string|Date} startTime - 开始时间
 * @param {string|Date} endTime - 结束时间
 * @returns {string} 格式化后的时间范围
 */
function formatTimeRange(startTime, endTime) {
    const start = formatTimeUTC8(startTime, 'datetime', false);
    const end = formatTimeUTC8(endTime, 'datetime', false);
    return `${start} ~ ${end} (UTC+8)`;
}

/**
 * 格式化持仓时长
 * @param {string|Date} openTime - 开仓时间
 * @param {string|Date} closeTime - 平仓时间（可选，默认为当前时间）
 * @returns {string} 持仓时长字符串（例如：2小时15分钟）
 */
function formatDuration(openTime, closeTime = null) {
    if (!openTime) return '-';

    try {
        const start = new Date(openTime);
        const end = closeTime ? new Date(closeTime) : new Date();

        const diffMs = end - start;
        const diffMinutes = Math.floor(diffMs / 1000 / 60);
        const diffHours = Math.floor(diffMinutes / 60);
        const diffDays = Math.floor(diffHours / 24);

        const remainingMinutes = diffMinutes % 60;
        const remainingHours = diffHours % 24;

        if (diffDays > 0) {
            return `${diffDays}天${remainingHours}小时`;
        } else if (diffHours > 0) {
            return `${diffHours}小时${remainingMinutes}分钟`;
        } else {
            return `${diffMinutes}分钟`;
        }
    } catch (e) {
        console.error('时长格式化失败:', e, openTime, closeTime);
        return '-';
    }
}

/**
 * 本地时间原样返回（后端存本地时间，无需转换）
 * @param {string|Date} utc8Time - 本地时间
 * @returns {string} 本地时间字符串
 */
function convertToUTC(utc8Time) {
    if (!utc8Time) return null;
    try {
        const s = typeof utc8Time === 'string'
            ? utc8Time.replace(' ', 'T').replace(/Z$/, '').replace(/[+-]\d{2}:\d{2}$/, '')
            : utc8Time instanceof Date ? utc8Time.toISOString().replace('Z', '') : String(utc8Time);
        return s;
    } catch (e) {
        console.error('时间转换失败:', e, utc8Time);
        return null;
    }
}

// 兼容旧代码的函数名（逐步迁移）
function formatTime(time) {
    return formatTimeUTC8(time, 'datetime', true);
}

function formatDate(time) {
    return formatTimeUTC8(time, 'date', false);
}

// 导出函数（用于ES6模块）
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        formatTimeUTC8,
        formatRelativeTime,
        formatTimeRange,
        formatDuration,
        convertToUTC,
        formatTime,
        formatDate
    };
}
