from message_processor.ceda import CEDAMessageProcessor
from message_processor.globus import GlobusMessageProcessor
from settings import settings

message_processor = (
    CEDAMessageProcessor() if settings.node == "ceda" else GlobusMessageProcessor()
)
