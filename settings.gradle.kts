rootProject.name = "kafka-connect-toolkit"

include("modules")
include("modules:fixtures")
include("modules:fixtures:fixtures-jar")
include("modules:fixtures:fixtures-kafka")
include("modules:fixtures:fixtures-debezium")
include("modules:fixtures:fixtures-postgres")

include("modules:transforms")

include("modules:converters")
include("modules:converters:converters-debezium-timestamp")
include("modules:transforms:transforms-debezium-schema-rename")
