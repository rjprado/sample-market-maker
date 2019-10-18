from decimal import Decimal
import math

def toNearest(num, tickSize):
    """Given a number, round it to the nearest tick. Very useful for sussing float error
       out of numbers: e.g. toNearest(401.46, 0.01) -> 401.46, whereas processing is
       normally with floats would give you 401.46000000000004.
       Use this after adding/subtracting/multiplying numbers."""
    tickDec = Decimal(str(tickSize))
    return float((Decimal(round(num / tickSize, 0)) * tickDec))

def toNearestCeil(num, tickSize):
    tickDec = Decimal(str(tickSize))
    return float((Decimal(math.ceil(num / tickSize)) * tickDec))

def toNearestFloor(num, tickSize):
    tickDec = Decimal(str(tickSize))
    return float((Decimal(math.floor(num / tickSize)) * tickDec))