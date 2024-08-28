from beta9 import endpoint


def load():
    return 5


@endpoint(
    cpu=1.0,
    on_start=load,
    workers=1
)
def multiply(context, x):
    factor = context.on_start_value
    result = x * factor
    return {"result": result}
