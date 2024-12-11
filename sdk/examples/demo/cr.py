from beta9 import Image, endpoint


def load():
    from transformers import AutoModelForCausalLM, AutoTokenizer

    model = AutoModelForCausalLM.from_pretrained('gpt2')
    tokenizer = AutoTokenizer.from_pretrained('gpt2')

    return model, tokenizer


@endpoint(
    # gpu='any',
    # memory='12Gi',
    cpu=1,
    on_start=load,
    workers=1,
    image=(Image(python_packages=['transformers', 'torch'])),
    checkpoint_enabled=True,
)
def inference(context, prompt):
    model, tokenizer = context.on_start_value

    # Generate
    inputs = tokenizer(prompt, return_tensors='pt')
    generate_ids = model.generate(inputs.input_ids, max_length=30)
    result = tokenizer.batch_decode(
        generate_ids,
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False,
    )[0]

    print(result)

    return {'prediction': result}
