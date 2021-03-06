from enum import Enum

class RegressionTypeEnum(Enum):
    MentorBusiness = "mentorbusiness"
    MentorBusinessV3 = "mentorbusinessv3"
    NonArmada = "non-armada"
    GEOTAB = "geotab"
    TLM112NONEGEOTAB = "tlm112-non-geotab"
    TLM112GEOTAB = 'tlm112-geotab'


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
    POOL_NONARMADA_1000="non-armada/1000"
    POOL_NONARMADA_10000 = "non-armada/10000"
    POOL_MENTORV3_10000 = "mentorbusinessv3/10000"
    POOL_MENTORV3_1000 = "mentorbusinessv3/1000"
    POOL_GEOTAB = "geotab"


class JSONfilenameEnum(Enum):
    base = "basefiles"
    file = "files"

class IdenticalJSONReportEnum(Enum):
    Yes="Y"
    No="N"