# Создайте файл task_scheduler.py
import asyncio
import logging
import time
from typing import Dict, Any, Callable, Coroutine, List, Optional, Set, Tuple, Union
from enum import Enum
from dataclasses import dataclass
import heapq

logger = logging.getLogger(__name__)

class TaskPriority(Enum):
    """Приоритеты задач."""
    HIGH = 0
    NORMAL = 1
    LOW = 2

@dataclass
class ScheduledTask:
    """Класс для хранения информации о запланированной задаче."""
    id: str
    func: Callable[..., Coroutine]
    args: Tuple
    kwargs: Dict[str, Any]
    next_run: float
    interval: Optional[float]
    priority: TaskPriority
    
    def __lt__(self, other):
        """Для сортировки в очереди приоритетов (heap)."""
        # Сортируем по времени выполнения, затем по приоритету
        if self.next_run == other.next_run:
            return self.priority.value < other.priority.value
        return self.next_run < other.next_run

class TaskScheduler:
    """
    Планировщик задач с поддержкой приоритетов и периодического выполнения.
    """
    
    def __init__(self, max_concurrent_tasks=5):
        """
        Инициализирует планировщик задач.
        
        Args:
            max_concurrent_tasks: Максимальное количество одновременных задач
        """
        self.tasks_queue = []  # heapq для задач
        self.running_tasks: Set[asyncio.Task] = set()
        self.task_map: Dict[str, ScheduledTask] = {}  # id -> task
        self.max_concurrent_tasks = max_concurrent_tasks
        self.stop_event = asyncio.Event()
        self.scheduler_task = None
        logger.info(f"Планировщик задач инициализирован (макс. {max_concurrent_tasks} задач)")
    
    def start(self):
        """Запускает планировщик задач."""
        if self.scheduler_task is None or self.scheduler_task.done():
            self.stop_event.clear()
            self.scheduler_task = asyncio.create_task(self._scheduler_loop())
            logger.info("Планировщик задач запущен")
    
    def stop(self):
        """Останавливает планировщик задач."""
        if self.scheduler_task and not self.scheduler_task.done():
            self.stop_event.set()
            logger.info("Планировщик задач остановлен")
    
    async def _scheduler_loop(self):
        """Основной цикл планировщика."""
        try:
            while not self.stop_event.is_set():
                await self._process_tasks()
                await asyncio.sleep(0.1)  # Небольшая пауза для снижения нагрузки
        except Exception as e:
            logger.error(f"Ошибка в цикле планировщика: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _process_tasks(self):
        """Обрабатывает задачи из очереди."""
        try:
            current_time = time.time()
            
            # Проверяем, есть ли задачи для выполнения
            while self.tasks_queue and self.tasks_queue[0].next_run <= current_time:
                # Проверяем, не превышено ли максимальное количество одновременных задач
                if len(self.running_tasks) >= self.max_concurrent_tasks:
                    break
                
                # Получаем задачу с наивысшим приоритетом
                task_info = heapq.heappop(self.tasks_queue)
                
                # Запускаем задачу
                asyncio_task = asyncio.create_task(
                    self._run_task(task_info)
                )
                self.running_tasks.add(asyncio_task)
                # Добавляем обработчик завершения
                asyncio_task.add_done_callback(self._task_done_callback)
                
                # Если это периодическая задача, планируем следующий запуск
                if task_info.interval is not None:
                    new_task_info = ScheduledTask(
                        id=task_info.id,
                        func=task_info.func,
                        args=task_info.args,
                        kwargs=task_info.kwargs,
                        next_run=current_time + task_info.interval,
                        interval=task_info.interval,
                        priority=task_info.priority
                    )
                    heapq.heappush(self.tasks_queue, new_task_info)
                    self.task_map[task_info.id] = new_task_info
                else:
                    # Если это не периодическая задача, удаляем из карты
                    self.task_map.pop(task_info.id, None)
        
        except Exception as e:
            logger.error(f"Ошибка при обработке задач: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _run_task(self, task_info: ScheduledTask):
        """
        Выполняет задачу.
        
        Args:
            task_info: Информация о задаче
        """
        try:
            logger.debug(f"Выполнение задачи {task_info.id}")
            await task_info.func(*task_info.args, **task_info.kwargs)
            logger.debug(f"Задача {task_info.id} выполнена успешно")
        except Exception as e:
            logger.error(f"Ошибка при выполнении задачи {task_info.id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _task_done_callback(self, task: asyncio.Task):
        """
        Обработчик завершения задачи.
        
        Args:
            task: Задача
        """
        self.running_tasks.discard(task)
    
    def schedule_task(
        self,
        task_id: str,
        func: Callable[..., Coroutine],
        delay: float = 0,
        interval: Optional[float] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        *args,
        **kwargs
    ) -> str:
        """
        Планирует задачу на выполнение.
        
        Args:
            task_id: Идентификатор задачи
            func: Функция для выполнения
            delay: Задержка перед первым выполнением (в секундах)
            interval: Интервал для периодического выполнения (в секундах)
            priority: Приоритет задачи
            *args: Позиционные аргументы для функции
            **kwargs: Именованные аргументы для функции
            
        Returns:
            Идентификатор задачи
        """
        # Если задача с таким ID уже существует, удаляем её
        self.cancel_task(task_id)
        
        # Создаем информацию о задаче
        next_run = time.time() + delay
        task_info = ScheduledTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            next_run=next_run,
            interval=interval,
            priority=priority
        )
        
        # Добавляем задачу в очередь и карту
        heapq.heappush(self.tasks_queue, task_info)
        self.task_map[task_id] = task_info
        
        logger.info(f"Задача {task_id} запланирована (delay={delay}s, interval={interval}s, priority={priority.name})")
        
        return task_id
    
    def cancel_task(self, task_id: str) -> bool:
        """
        Отменяет задачу по идентификатору.
        
        Args:
            task_id: Идентификатор задачи
            
        Returns:
            True, если задача найдена и отменена, иначе False
        """
        if task_id in self.task_map:
            # Удаляем задачу из карты
            task_info = self.task_map.pop(task_id)
            
            # Создаем новый список задач без отмененной задачи
            self.tasks_queue = [task for task in self.tasks_queue if task.id != task_id]
            heapq.heapify(self.tasks_queue)
            
            logger.info(f"Задача {task_id} отменена")
            return True
            
        return False
    
    def reschedule_task(self, task_id: str, next_run: float) -> bool:
        """
        Изменяет время следующего выполнения задачи.
        
        Args:
            task_id: Идентификатор задачи
            next_run: Новое время выполнения (timestamp)
            
        Returns:
            True, если задача найдена и перепланирована, иначе False
        """
        if task_id in self.task_map:
            # Получаем существующую задачу
            task_info = self.task_map[task_id]
            
            # Создаем новую задачу с обновленным временем выполнения
            new_task_info = ScheduledTask(
                id=task_info.id,
                func=task_info.func,
                args=task_info.args,
                kwargs=task_info.kwargs,
                next_run=next_run,
                interval=task_info.interval,
                priority=task_info.priority
            )
            
            # Отменяем старую задачу и добавляем новую
            self.cancel_task(task_id)
            heapq.heappush(self.tasks_queue, new_task_info)
            self.task_map[task_id] = new_task_info
            
            logger.info(f"Задача {task_id} перепланирована на {next_run}")
            return True
            
        return False
    
    def get_pending_tasks_count(self) -> int:
        """
        Возвращает количество задач в очереди.
        
        Returns:
            Количество задач
        """
        return len(self.tasks_queue)
    
    def get_running_tasks_count(self) -> int:
        """
        Возвращает количество выполняющихся задач.
        
        Returns:
            Количество задач
        """
        return len(self.running_tasks)
    
    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """
        Возвращает информацию о всех задачах.
        
        Returns:
            Список информации о задачах
        """
        all_tasks = []
        
        # Добавляем задачи из очереди
        for task in self.tasks_queue:
            all_tasks.append({
                'id': task.id,
                'next_run': task.next_run,
                'interval': task.interval,
                'priority': task.priority.name,
                'status': 'pending'
            })
        
        # Добавляем выполняющиеся задачи
        for task in self.running_tasks:
            # Пытаемся получить идентификатор задачи
            task_id = getattr(task, 'id', f'task_{id(task)}')
            all_tasks.append({
                'id': task_id,
                'next_run': 0,
                'interval': None,
                'priority': 'RUNNING',
                'status': 'running'
            })
        
        return all_tasks

# Создаем глобальный экземпляр планировщика
scheduler = TaskScheduler()