from settings import settings

if settings.node == "ceda":
    from message_processor.ceda import CEDAMessageProcessor as mp
else:
    from message_processor.globus import GlobusMessageProcessor as mp

message_processor = mp()
