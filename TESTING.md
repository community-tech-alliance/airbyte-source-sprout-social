# Testing locally

There are two ways you can test your connector locally:

1) Running `python main.py [COMMAND] [ARGUMENTS]`. This is the fastest way to test your code.
2) Building the Docker image using `docker build . -t [IMAGE:TAG]` and then running it using `docker run --rm [IMAGE:TAG] [COMMAND] [ARGS]`. This is slightly slower but in theory replicates _exactly_ what your connector will be doing when run in Airbyte.

## Prerequisite 1: Secrets file

To run every command except `spec`, you need to pass in the argument `--config [PATH_TO_SECRETS_CONFIG]`, for example `--config secrets/config.json`. (All files in `secrets/` are ignored by the .gitignore.) 

The config json contains key-value pairs for any credentials needed to connect. For a source that only needs a single API key (defined in the `spec` file with the name `api_key`), the config.json file will look like this:

```text
{
  "api_key": "beepbopboop"
}
```

## Prerequisite 2: Configured Catalog

To test the `read` command, which actually reads data, you need to also pass in a path to a file containing the "configured catalog," which defines the streams and their configurations to be run. This is a JSON configuration file that defines the streams to be read from the source, along with any stream-specific configurations (like if the data should be synced with full refresh or incremental updates).

You can use different JSON files to test out different streams individually. Here's an example of a catalog file that only tests a single stream, called "customers":

```text
{
  "streams": [
    {
      "stream": {
        "name": "client_metadata",
        "json_schema": {},
        "supported_sync_modes": ["full_refresh"]
      },
      "sync_mode": "full_refresh",
      "destination_sync_mode": "overwrite"
    }
  ]
}

```

## Prerequisite 3: Schema files

Each stream must have a JSON present in the `schemas` folder. For example, [client_metadata.json](source_sprout_social/schemas/client_metadata.json) defines the schema for the ClientMetadata stream (Airbyte understands that the snake case in the JSON file corresponds to the camel-cased Class name), which is simple and only contains one field (most will have more than one):

```text
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "customer_id": {
      "type": ["string"]
    }
  }
}
```

## Python cheat sheet

```shell
python main.py spec
python main.py check --config secrets/config.json
python main.py discover --config secrets/config.json
python main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

## Docker cheat sheet

Every time you want to test your code using Docker, you need to rebuild your Docker image, for example:

```shell
docker build . -t communitytechalliance/source-sprout-social:dev
```

Then you run commands just like in the Python cheat sheet above, but running the Docker image instead of `main.py`:

```shell
docker run --rm communitytechalliance/source-sprout-social:dev spec
docker run --rm -v $(pwd)/secrets:/secrets communitytechalliance/source-sprout-social:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets communitytechalliance/source-sprout-social:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests communitytechalliance/source-sprout-social:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

# Testing your connector in Airbyte

To test your connector in its natural habitat (Airbyte), you need to push the image to CTA's public dockerhub account, and then configure a new source in Airbyte using that image name and tag.

To authenticate to dockerhub (look for CTA's credentials in 1Password):

```shell
docker login
```

Assuming you have already built your image with the name `communitytechalliance/SOURCE-NAME:TAG`, next you need to push it to dockerhub:

```shell
docker push communitytechalliance/source-sprout-social:dev
```

And finally, go into the Airbyte UI, create a new source using that docker image and tag, create a connection with a BigQuery destination, and see if you can sync some data!