from enum import StrEnum


class NeuronType(StrEnum):
    Miner = "miner"
    Validator = "validator"


class ImageFormat(StrEnum):
    PNG = "png"
    JPEG = "jpeg"


PRESIGN_EXPIRY = 604800  # 7 days in seconds
