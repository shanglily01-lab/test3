"""
Hyperliquid Token Mapper
将 Hyperliquid 的 @N 代币索引映射为可读的代币符号
"""
import logging
import json
import requests
from typing import Dict, Optional
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


class HyperliquidTokenMapper:
    """Hyperliquid代币映射服务"""

    def __init__(self, cache_file: str = None):
        """
        初始化代币映射器

        Args:
            cache_file: 缓存文件路径，默认为 data/hyperliquid_tokens.json
        """
        if cache_file is None:
            cache_file = Path(__file__).parent.parent.parent / "data" / "hyperliquid_tokens.json"

        self.cache_file = Path(cache_file)
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)

        self.api_url = "https://api.hyperliquid.xyz/info"
        self.token_map: Dict[str, str] = {}  # @N -> symbol
        self.reverse_map: Dict[str, str] = {}  # symbol -> @N
        self.last_update: Optional[datetime] = None
        self.cache_duration = timedelta(hours=24)  # 缓存24小时

        # 加载缓存
        self._load_cache()

    def _load_cache(self) -> bool:
        """从缓存文件加载代币映射"""
        try:
            if not self.cache_file.exists():
                logger.info("缓存文件不存在，将首次获取代币映射")
                return False

            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            self.token_map = cache_data.get('token_map', {})
            self.reverse_map = cache_data.get('reverse_map', {})
            last_update_str = cache_data.get('last_update')

            if last_update_str:
                self.last_update = datetime.fromisoformat(last_update_str)
                logger.info(f"从缓存加载 {len(self.token_map)} 个代币映射，最后更新: {self.last_update}")
                return True

            return False

        except Exception as e:
            logger.error(f"加载缓存失败: {e}")
            return False

    def _save_cache(self) -> bool:
        """保存代币映射到缓存文件"""
        try:
            cache_data = {
                'token_map': self.token_map,
                'reverse_map': self.reverse_map,
                'last_update': self.last_update.isoformat() if self.last_update else None,
                'total_tokens': len(self.token_map)
            }

            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            logger.info(f"保存 {len(self.token_map)} 个代币映射到缓存")
            return True

        except Exception as e:
            logger.error(f"保存缓存失败: {e}")
            return False

    def _need_update(self) -> bool:
        """检查是否需要更新映射"""
        if not self.token_map:
            return True

        if self.last_update is None:
            return True

        if datetime.now() - self.last_update > self.cache_duration:
            return True

        return False

    def update_token_mapping(self, force: bool = False) -> bool:
        """
        从Hyperliquid API更新代币映射

        Args:
            force: 是否强制更新（忽略缓存时间）

        Returns:
            bool: 更新成功返回True
        """
        if not force and not self._need_update():
            logger.info("代币映射仍在有效期内，跳过更新")
            return True

        try:
            logger.info("正在从Hyperliquid API获取代币映射...")

            # 请求所有代币的元数据
            response = requests.post(
                self.api_url,
                json={"type": "metaAndAssetCtxs"},
                timeout=10
            )

            if response.status_code != 200:
                logger.error(f"API请求失败: {response.status_code}")
                return False

            data = response.json()

            # 解析代币信息
            if not isinstance(data, list) or len(data) < 1:
                logger.error("API返回数据格式错误")
                return False

            meta = data[0].get('universe', [])

            # 构建映射
            new_token_map = {}
            new_reverse_map = {}

            for idx, token_info in enumerate(meta):
                symbol = token_info.get('name', '')
                if symbol:
                    index_key = f"@{idx}"
                    new_token_map[index_key] = symbol
                    new_reverse_map[symbol] = index_key

            if not new_token_map:
                logger.error("未能解析到任何代币信息")
                return False

            # 更新映射
            self.token_map = new_token_map
            self.reverse_map = new_reverse_map
            self.last_update = datetime.now()

            logger.info(f"成功更新 {len(self.token_map)} 个代币映射")

            # 保存缓存
            self._save_cache()

            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"网络请求失败: {e}")
            return False
        except Exception as e:
            logger.error(f"更新代币映射失败: {e}")
            return False

    def get_symbol(self, index: str) -> str:
        """
        获取代币符号

        Args:
            index: 代币索引，如 "@107" 或 "107"

        Returns:
            str: 代币符号，如果未找到则返回原始索引
        """
        # 确保映射已加载
        if self._need_update():
            self.update_token_mapping()

        # 标准化索引格式
        if not index.startswith('@'):
            index = f"@{index}"

        return self.token_map.get(index, index)

    def get_index(self, symbol: str) -> Optional[str]:
        """
        获取代币索引

        Args:
            symbol: 代币符号，如 "ALT"

        Returns:
            str: 代币索引，如 "@107"，未找到返回 None
        """
        # 确保映射已加载
        if self._need_update():
            self.update_token_mapping()

        return self.reverse_map.get(symbol.upper())

    def format_symbol(self, symbol: str) -> str:
        """
        格式化代币符号显示

        Args:
            symbol: 原始符号（可能是@N或正常符号）

        Returns:
            str: 格式化后的显示文本
            例如: "@107" -> "ALT (@107)"
                 "BTC" -> "BTC"
        """
        if not symbol.startswith('@'):
            return symbol

        # 获取真实符号
        real_symbol = self.get_symbol(symbol)

        # 如果找到了映射，显示为 "符号 (索引)"
        if real_symbol != symbol:
            return f"{real_symbol} ({symbol})"

        # 未找到映射，只显示索引
        return symbol

    def get_all_tokens(self) -> Dict[str, str]:
        """
        获取所有代币映射

        Returns:
            Dict[str, str]: 完整的代币映射字典
        """
        if self._need_update():
            self.update_token_mapping()

        return self.token_map.copy()

    def search_tokens(self, keyword: str) -> Dict[str, str]:
        """
        搜索代币

        Args:
            keyword: 搜索关键词

        Returns:
            Dict[str, str]: 匹配的代币映射
        """
        if self._need_update():
            self.update_token_mapping()

        keyword = keyword.upper()
        results = {}

        for index, symbol in self.token_map.items():
            if keyword in symbol.upper() or keyword in index:
                results[index] = symbol

        return results

    def get_stats(self) -> Dict:
        """
        获取映射统计信息

        Returns:
            Dict: 统计信息
        """
        return {
            'total_tokens': len(self.token_map),
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'cache_file': str(self.cache_file),
            'cache_valid': not self._need_update()
        }


# 全局单例
_mapper_instance: Optional[HyperliquidTokenMapper] = None


def get_token_mapper() -> HyperliquidTokenMapper:
    """获取全局代币映射器实例"""
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = HyperliquidTokenMapper()
    return _mapper_instance


def format_hyperliquid_symbol(symbol: str) -> str:
    """
    便捷函数：格式化Hyperliquid代币符号

    Args:
        symbol: 原始符号

    Returns:
        str: 格式化后的符号
    """
    mapper = get_token_mapper()
    return mapper.format_symbol(symbol)


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.INFO)

    mapper = HyperliquidTokenMapper()

    # 更新映射
    print("🔄 更新代币映射...")
    success = mapper.update_token_mapping(force=True)

    if success:
        print(f"\n✅ 成功获取 {len(mapper.token_map)} 个代币映射\n")

        # 测试几个索引
        test_indices = ["@0", "@1", "@107", "@200"]
        print("📊 测试索引转换:")
        for idx in test_indices:
            symbol = mapper.get_symbol(idx)
            formatted = mapper.format_symbol(idx)
            print(f"  {idx} -> {symbol} (显示: {formatted})")

        # 测试反向查询
        print("\n🔍 测试符号查询:")
        test_symbols = ["BTC", "ETH", "ALT", "SOL"]
        for sym in test_symbols:
            idx = mapper.get_index(sym)
            print(f"  {sym} -> {idx}")

        # 显示统计
        print("\n📈 统计信息:")
        stats = mapper.get_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # 显示前20个代币
        print("\n📋 前20个代币:")
        for i in range(min(20, len(mapper.token_map))):
            idx = f"@{i}"
            symbol = mapper.get_symbol(idx)
            print(f"  {idx}: {symbol}")
    else:
        print("❌ 更新失败")
