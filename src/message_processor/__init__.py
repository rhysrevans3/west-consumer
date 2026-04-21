from src.message_processor.ceda import CEDAMessageProcessor
from src.message_processor.globus import GlobusMessageProcessor
from src.settings import settings

message_processor = (
    CEDAMessageProcessor() if settings.node == "ceda" else GlobusMessageProcessor()
)
