import tomli

with open("./jobparams.toml", "rb") as f:
    toml_dict = tomli.load(f)

print(toml_dict.get("jobparams"))
