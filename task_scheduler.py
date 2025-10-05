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

    def __str__(self):
        return f"Task {self.id}: Duration={self.duration}min, Priority={self.priority}, Dependencies={self.dependencies}, Deadline={self.deadline}"

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
                print(f"⚠️  Warning: Task {task.id} might miss deadline!")
                
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

def get_user_input():
    """Get task details from user input"""
    tasks = []
    
    print("🎯 Task Scheduling System - User Input")
    print("=" * 50)
    
    while True:
        print(f"\n📝 Entering Task #{len(tasks) + 1}")
        print("-" * 30)
        
        # Task ID
        task_id = input("Enter Task ID (e.g., T1, TaskA): ").strip()
        if not task_id:
            print("Task ID cannot be empty!")
            continue
            
        # Duration
        try:
            duration = int(input("Enter task duration (in minutes): "))
            if duration <= 0:
                print("Duration must be positive!")
                continue
        except ValueError:
            print("Please enter a valid number for duration!")
            continue
            
        # Priority
        try:
            priority = int(input("Enter priority (1-10, where 10 is highest): "))
            if priority < 1 or priority > 10:
                print("Priority must be between 1 and 10!")
                continue
        except ValueError:
            print("Please enter a valid number for priority!")
            continue
            
        # Dependencies
        dependencies_input = input("Enter dependencies (comma-separated, e.g., T1,T2 or press Enter for none): ").strip()
        dependencies = [dep.strip() for dep in dependencies_input.split(',')] if dependencies_input else []
        
        # Deadline
        deadline = None
        deadline_input = input("Enter deadline (YYYY-MM-DD HH:MM format or press Enter for no deadline): ").strip()
        if deadline_input:
            try:
                deadline = datetime.strptime(deadline_input, "%Y-%m-%d %H:%M")
                if deadline < datetime.now():
                    print("Warning: Deadline is in the past!")
            except ValueError:
                print("Invalid date format! Using no deadline.")
                deadline = None
        
        # Create task
        task = Task(
            id=task_id,
            duration=duration,
            priority=priority,
            dependencies=dependencies,
            deadline=deadline
        )
        
        tasks.append(task)
        
        # Ask if user wants to add more tasks
        more = input("\nAdd another task? (y/n): ").strip().lower()
        if more not in ['y', 'yes']:
            break
    
    return tasks

def display_schedule(scheduled_tasks: List[Task]):
    """Display the scheduled tasks in a formatted way"""
    print("\n" + "📅 FINAL SCHEDULE".center(80, "="))
    print(f"{'Task':<10} {'Start Time':<20} {'End Time':<20} {'Duration':<10} {'Status':<12}")
    print("-" * 80)
    
    for task in scheduled_tasks:
        start_str = task.start_time.strftime("%Y-%m-%d %H:%M") if task.start_time else "N/A"
        end_str = task.end_time.strftime("%Y-%m-%d %H:%M") if task.end_time else "N/A"
        print(f"{task.id:<10} {start_str:<20} {end_str:<20} {task.duration:<10} {task.status.value:<12}")

def display_metrics(metrics: Dict[str, Any]):
    """Display performance metrics in a formatted way"""
    print("\n" + "📊 PERFORMANCE METRICS".center(50, "="))
    for key, value in metrics.items():
        if isinstance(value, float):
            formatted_value = f"{value:.2f}"
        else:
            formatted_value = str(value)
        
        # Format key for display
        display_key = key.replace('_', ' ').title()
        print(f"• {display_key}: {formatted_value}")

def main():
    """Main function with interactive user input"""
    print("🚀 Welcome to the Task Scheduling System!")
    print("This system will help you schedule your tasks efficiently.\n")
    
    while True:
        print("Choose an option:")
        print("1. 📝 Input your own tasks")
        print("2. 🧪 Run with sample tasks")
        print("3. ❌ Exit")
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == '1':
            # User input mode
            tasks = get_user_input()
            
            if not tasks:
                print("No tasks were entered. Please try again.")
                continue
                
            scheduler = TaskScheduler()
            
            # Add all tasks to scheduler
            for task in tasks:
                scheduler.add_task(task)
                print(f"✅ Added: {task}")
            
            # Schedule tasks
            print("\n⏳ Scheduling tasks...")
            scheduled_tasks = scheduler.schedule_tasks()
            
            # Display results
            display_schedule(scheduled_tasks)
            
            # Calculate and display metrics
            metrics = scheduler.calculate_metrics(scheduled_tasks)
            display_metrics(metrics)
            
        elif choice == '2':
            # Sample tasks mode
            print("\n🧪 Running with sample tasks...")
            
            scheduler = TaskScheduler()
            
            # Create sample tasks
            sample_tasks = [
                Task("T1", 30, priority=5, deadline=datetime.now() + timedelta(hours=2)),
                Task("T2", 45, priority=8, dependencies=["T1"], deadline=datetime.now() + timedelta(hours=3)),
                Task("T3", 20, priority=3, deadline=datetime.now() + timedelta(hours=1)),
                Task("T4", 60, priority=6, dependencies=["T2"], deadline=datetime.now() + timedelta(hours=4)),
                Task("T5", 15, priority=9, deadline=datetime.now() + timedelta(hours=1)),
            ]
            
            for task in sample_tasks:
                scheduler.add_task(task)
                print(f"✅ Added: {task}")
            
            # Schedule tasks
            print("\n⏳ Scheduling tasks...")
            scheduled_tasks = scheduler.schedule_tasks()
            
            # Display results
            display_schedule(scheduled_tasks)
            
            # Calculate and display metrics
            metrics = scheduler.calculate_metrics(scheduled_tasks)
            display_metrics(metrics)
            
        elif choice == '3':
            print("👋 Thank you for using the Task Scheduling System! Goodbye!")
            break
            
        else:
            print("❌ Invalid choice! Please enter 1, 2, or 3.")
        
        # Ask if user wants to continue
        if choice in ['1', '2']:
            continue_choice = input("\n🔄 Would you like to schedule more tasks? (y/n): ").strip().lower()
            if continue_choice not in ['y', 'yes']:
                print("👋 Thank you for using the Task Scheduling System! Goodbye!")
                break

if __name__ == "__main__":
    main()
