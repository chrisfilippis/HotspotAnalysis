import math


class Helpers(object):
    @staticmethod
    def ceil_string_value(string_value, step):
        return Helpers.find_ceil_value(float(string_value), step)

    @staticmethod
    def find_ceil_value(lat, step):
        return math.ceil(lat / step)
