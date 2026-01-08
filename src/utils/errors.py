# src/utils/errors.py
class UserInputError(RuntimeError):
    """
    Raised for invalid user-provided config (dates, symbols, etc).
    Should NOT print traceback.
    """
