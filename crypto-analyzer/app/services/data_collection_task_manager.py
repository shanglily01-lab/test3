"""
数据采集任务管理器
用于管理后台数据采集任务，支持进度跟踪
"""

import asyncio
import uuid
from datetime import datetime
from typing import Dict, Optional, List
from enum import Enum
from loguru import logger
import threading


class TaskStatus(str, Enum):
    """任务状态"""
    PENDING = "pending"  # 等待中
    RUNNING = "running"  # 运行中
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"  # 失败
    CANCELLED = "cancelled"  # 已取消


class DataCollectionTask:
    """数据采集任务"""
    
    def __init__(self, task_id: str, request_data: Dict):
        self.task_id = task_id
        self.request_data = request_data
        self.status = TaskStatus.PENDING
        self.progress = 0.0  # 0-100
        self.current_step = ""  # 当前步骤描述
        self.total_steps = 0  # 总步骤数
        self.completed_steps = 0  # 已完成步骤数
        self.total_saved = 0  # 总保存数据量
        self.estimated_total_records = 0  # 预估总数据量（用于进度计算）
        self.errors = []  # 错误列表
        self.result = None  # 最终结果
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.task: Optional[asyncio.Task] = None
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'task_id': self.task_id,
            'status': self.status.value,
            'progress': round(self.progress, 2),
            'current_step': self.current_step,
            'total_steps': self.total_steps,
            'completed_steps': self.completed_steps,
            'total_saved': self.total_saved,
            'estimated_total_records': getattr(self, 'estimated_total_records', 0),
            'errors': self.errors,
            'result': self.result,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'request_data': self.request_data
        }


class DataCollectionTaskManager:
    """数据采集任务管理器（单例）"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.tasks: Dict[str, DataCollectionTask] = {}
        self._lock = threading.Lock()
        logger.info("数据采集任务管理器已初始化")
    
    def create_task(self, request_data: Dict) -> str:
        """创建新任务"""
        task_id = str(uuid.uuid4())
        task = DataCollectionTask(task_id, request_data)
        
        with self._lock:
            self.tasks[task_id] = task
        
        logger.info(f"创建数据采集任务: {task_id}")
        return task_id
    
    def get_task(self, task_id: str) -> Optional[DataCollectionTask]:
        """获取任务"""
        with self._lock:
            return self.tasks.get(task_id)
    
    def update_task_progress(
        self,
        task_id: str,
        progress: float = None,
        current_step: str = None,
        completed_steps: int = None,
        total_saved: int = None,
        error: str = None
    ):
        """更新任务进度"""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return
            
            if progress is not None:
                task.progress = min(100.0, max(0.0, progress))
            if current_step is not None:
                task.current_step = current_step
            if completed_steps is not None:
                task.completed_steps = completed_steps
            if total_saved is not None:
                task.total_saved = total_saved
            if error:
                task.errors.append(error)
    
    def set_task_status(self, task_id: str, status: TaskStatus, result: Dict = None):
        """设置任务状态"""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return
            
            task.status = status
            if status == TaskStatus.RUNNING and not task.started_at:
                task.started_at = datetime.now()
            elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                task.completed_at = datetime.now()
                task.progress = 100.0 if status == TaskStatus.COMPLETED else task.progress
            if result:
                task.result = result
    
    def get_all_tasks(self, limit: int = 50) -> List[Dict]:
        """获取所有任务（最近N个）"""
        with self._lock:
            tasks = list(self.tasks.values())
            # 按创建时间倒序排列
            tasks.sort(key=lambda t: t.created_at, reverse=True)
            return [task.to_dict() for task in tasks[:limit]]
    
    def cleanup_old_tasks(self, days: int = 7):
        """清理旧任务（保留最近N天）"""
        cutoff = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        with self._lock:
            to_remove = []
            for task_id, task in self.tasks.items():
                if task.created_at.timestamp() < cutoff:
                    to_remove.append(task_id)
            
            for task_id in to_remove:
                del self.tasks[task_id]
            
            if to_remove:
                logger.info(f"清理了 {len(to_remove)} 个旧任务")


# 全局任务管理器实例
task_manager = DataCollectionTaskManager()

