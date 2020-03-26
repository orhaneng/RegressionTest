from enum import Enum

class RegressionTypeEnum(Enum):
    MentorBusiness = "mentorbusiness"
    NonArmada = "non-armada"
    GEOTAB = "geotab"
    TLM112 = "tlm112"


class RegressionProcessTypeEnum(Enum):
    RegressionTest = "1"
    RegressionUpdateBaseTripresults = "2"
    RegressionMapBase = "3"


class PoolSize(Enum):
    POOL_1000 = "1000"
    POOL_10000 = "10000"
    POOL_20000 = "20000"
    POOL_50000 = "50000"
    POOL_100000 = "100000"
    POOL_NONARMADA="non-armada"
    POOL_GEOTAB = "geotab"


class JSONfilenameEnum(Enum):
    base = "basefiles"
    file = "files"

class IdenticalJSONReportEnum(Enum):
    Yes="Y"
    No="N"