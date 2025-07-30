import os
from beta9.abstractions.function import Function as function
from beta9.abstractions.image import Image
from beta9.abstractions.volume import Volume

@function(
  image=Image.from_dockerfile("./Dockerfile"),
)
def run():
  print("sup sup")

run()
