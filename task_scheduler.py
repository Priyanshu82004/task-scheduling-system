import heapq
from datetime import datetime, timedelta
from typing import List, Dict, Any
from enum import Enum

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class Task:
    def __init__(self, id: str, duration: int, priority: int = 1, 
                 dependencies: List[str] = None, deadline: datetime = None):
        self.id = id
        self.duration = duration  # in minutes
        self.priority = priority  # 1-10, where 10 is highest
        self.dependencies = dependencies or []
        self.deadline = deadline
        self.status = TaskStatus.PENDING
        self.start_time = None
        self.end_time = None
        
    def __lt__(self, other):
        # For priority queue: higher priority tasks come first
        if self.priority == other.priority:
            if self.deadline and other.deadline:
                return self.deadline < other.deadline
            return self.duration < other.duration
        return self.priority > other.priority

class TaskScheduler:
    def __init__(self):
        self.tasks = {}
        self.completed_tasks = []
        self.current_time = datetime.now()
        
    def add_task(self, task: Task):
        """Add a task to the scheduler"""
        self.tasks[task.id] = task
        
    def remove_task(self, task_id: str):
        """Remove a task from the scheduler"""
        if task_id in self.tasks:
            del self.tasks[task_id]
            
    def get_ready_tasks(self) -> List[Task]:
        """Get tasks that have all dependencies satisfied"""
        ready_tasks = []
        for task in self.tasks.values():
            if task.status == TaskStatus.PENDING:
                # Check if all dependencies are completed
                dependencies_met = all(
                    dep_id in [t.id for t in self.completed_tasks] 
                    for dep_id in task.dependencies
                )
                if not task.dependencies or dependencies_met:
                    ready_tasks.append(task)
        return ready_tasks
    
    def schedule_tasks(self) -> List[Task]:
        """Schedule tasks using priority-based scheduling with deadline awareness"""
        scheduled_tasks = []
        ready_tasks = self.get_ready_tasks()
        
        # Create priority queue
        task_queue = []
        for task in ready_tasks:
            heapq.heappush(task_queue, task)
            
        current_time = self.current_time
        
        while task_queue:
            task = heapq.heappop(task_queue)
            
            # Check if task can meet deadline
            if task.deadline and current_time + timedelta(minutes=task.duration) > task.deadline:
                print(f"Warning: Task {task.id} might miss deadline")
                
            # Schedule the task
            task.start_time = current_time
            task.end_time = current_time + timedelta(minutes=task.duration)
            task.status = TaskStatus.RUNNING
            
            # Simulate task execution
            current_time = task.end_time
            task.status = TaskStatus.COMPLETED
            
            scheduled_tasks.append(task)
            self.completed_tasks.append(task)
            
            # Remove from active tasks
            del self.tasks[task.id]
            
            # Check for new ready tasks
            new_ready_tasks = self.get_ready_tasks()
            for new_task in new_ready_tasks:
                if new_task not in task_queue and new_task not in scheduled_tasks:
                    heapq.heappush(task_queue, new_task)
                    
        return scheduled_tasks
    
    def calculate_metrics(self, scheduled_tasks: List[Task]) -> Dict[str, Any]:
        """Calculate scheduling performance metrics"""
        total_tasks = len(scheduled_tasks)
        completed_on_time = 0
        total_tardiness = 0
        total_completion_time = 0
        
        for task in scheduled_tasks:
            if task.deadline and task.end_time <= task.deadline:
                completed_on_time += 1
                
            if task.deadline:
                tardiness = max(0, (task.end_time - task.deadline).total_seconds() / 60)
                total_tardiness += tardiness
                
            total_completion_time += task.duration
            
        return {
            "total_tasks": total_tasks,
            "completed_on_time": completed_on_time,
            "on_time_percentage": (completed_on_time / total_tasks * 100) if total_tasks > 0 else 0,
            "average_tardiness": total_tardiness / total_tasks if total_tasks > 0 else 0,
            "total_completion_time": total_completion_time,
            "makespan": (scheduled_tasks[-1].end_time - scheduled_tasks[0].start_time).total_seconds() / 60 if scheduled_tasks else 0
        }

# Example usage and testing
def main():
    scheduler = TaskScheduler()
    
    # Create sample tasks
    tasks = [
        Task("T1", 30, priority=5, deadline=datetime.now() + timedelta(hours=2)),
        Task("T2", 45, priority=8, dependencies=["T1"], deadline=datetime.now() + timedelta(hours=3)),
        Task("T3", 20, priority=3, deadline=datetime.now() + timedelta(hours=1)),
        Task("T4", 60, priority=6, dependencies=["T2"], deadline=datetime.now() + timedelta(hours=4)),
        Task("T5", 15, priority=9, deadline=datetime.now() + timedelta(hours=1)),
    ]
    
    for task in tasks:
        scheduler.add_task(task)
        
    # Schedule tasks
    scheduled = scheduler.schedule_tasks()
    
    # Print schedule
    print("Task Schedule:")
    print("-------------")
    for task in scheduled:
        print(f"Task {task.id}: {task.start_time.strftime('%H:%M')} - {task.end_time.strftime('%H:%M')} "
              f"(Duration: {task.duration}min, Status: {task.status.value})")
    
    # Print metrics
    metrics = scheduler.calculate_metrics(scheduled)
    print("\nScheduling Metrics:")
    print("------------------")
    for key, value in metrics.items():
        print(f"{key}: {value}")

if __name__ == "__main__":
    main()
