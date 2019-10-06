from enum import Enum

class RegressionTypeEnum(Enum):
    MentorBusiness = "1"
    NonArmada = "2"


class RegressionProcessTypeEnum(Enum):
    RegressionTest = "1"
    RegressionUpdateMainTripresults = "2"
    RegressionMapBase = "3"


class PoolSize(Enum):
    POOL_1000 = "1000"
    POOL_10000 = "10000"
    POOL_20000 = "20000"
    POOL_50000 = "50000"
    POOL_100000 = "100000"
    POOL_NANARMADA="non-armada"