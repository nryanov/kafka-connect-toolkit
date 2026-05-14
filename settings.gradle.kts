rootProject.name = "kafka-connect-toolkit"

include(":modules")
include(":modules:fixtures")
include(":modules:fixtures:fixtures-kafka")
include(":modules:fixtures:fixtures-debezium")
include(":modules:fixtures:fixtures-postgres")
include(":modules:fixtures:fixtures-kafka-connect")
include(":modules:fixtures:fixtures-schema-registry")

include(":modules:toolkit")
include(":modules:debezium")
include(":modules:debezium:debezium-postgres")

include(":modules:core")
