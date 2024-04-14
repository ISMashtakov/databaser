from dotenv import load_dotenv
load_dotenv()

from databaser.core.managers import (
    DatabaserManager,
)

if __name__ == '__main__':
    manager = DatabaserManager()
    manager.manage()
