from beam import VM

vm = VM(
    name="quickstart",
)

vm.create()
process = vm.process.exec("printf", "Hello from your VM!\n")
process.wait(30)
print(process.stdout.read())
