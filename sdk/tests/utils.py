# Helps mock out results that should return from a coroutine function
# that you want to override
def mock_coroutine_with_result(result):
    async def _result(*args, **kwargs):
        return result

    return _result
