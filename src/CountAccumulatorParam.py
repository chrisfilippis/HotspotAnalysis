from pyspark.accumulators import AccumulatorParam


class CountAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue=""):
        return ""

    def addInPlace(self, s1, s2):
        return s1.strip() + " " + s2.strip()