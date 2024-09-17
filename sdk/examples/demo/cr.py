from beta9 import endpoint, Image


def load():
    from transformers import AutoTokenizer, AutoModelForCausalLM

    model = AutoModelForCausalLM.from_pretrained("gpt2")
    tokenizer = AutoTokenizer.from_pretrained("gpt2")

    return model, tokenizer


@endpoint(
    cpu=1.0,
    gpu='any',
    on_start=load,
    workers=1,
    image=(
        Image(
            python_packages=[
                "transformers",
                "torch"
                ]
            )
        )
)
def inference(context, prompt):
    model, tokenizer = context.on_start_value

    # Generate
    inputs = tokenizer(prompt, return_tensors="pt")
    generate_ids = model.generate(inputs.input_ids, max_length=30)
    result = tokenizer.batch_decode(
        generate_ids, skip_special_tokens=True, clean_up_tokenization_spaces=False
    )[0]

    print(result)

    return {"prediction": result}
