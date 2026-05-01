from west_consumer.settings import settings

if settings.node == "ceda":
    from west_consumer.message_processor.ceda import CEDAMessageProcessor as mp
else:
    from west_consumer.message_processor.globus import GlobusMessageProcessor as mp

message_processor = mp()
