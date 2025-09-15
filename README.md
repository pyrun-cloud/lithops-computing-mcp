# Lithops-Computing

This guide provides two methods for running and interacting with the application.

---

## Method 1: Manual AWS Inbound Rule Setup

This method involves running the application directly and connecting to it from your local machine, which may require configuring a network rule in your AWS security group.

1.  **Execute the Application**

    On your PyRun VM, start the application server.

    ```bash
    python3 /work/app.py
    ```

2.  **Open the Inspector**

    From your personal PC, launch the Model-Context-Protocol Inspector.

    ```bash
    npx @modelcontextprotocol/inspector
    ```

3.  **Connect to the PyRun VM**

    In the Inspector UI, configure the connection with the following details:
    *   **Transport Type:** `streamable http`
    *   **URL:** `http://<IP_OF_VM>:8080/mcp`

    > **Note:** You may need to add an Inbound Rule to your VM's AWS Security Group to allow HTTP traffic on port `8080` from your IP address.

4.  **Example Usage**

    a. Execute `lithops_functionexecutor` without parameters.

    b. Run a `map` operation.

    ```python
    lithops_map(
          func="def my_function(x, y): return x + y",
          name_func="my_function",
          map_iterdata=[[1, 2], [3, 4], [5, 6]],
          tuple_list=True
    )
    # Expected result: [3, 7, 11]
    ```

    c. Fetch the first two results using `get_result`.

---

## Method 2: Using Ollama

This method uses `mcphost` to integrate with a locally running Ollama model, communicating via standard I/O instead of HTTP.

> **Prerequisites:** You must have Ollama installed, along with the language model you wish to use.

First, you must modify a line in the application code.

**Substitute this line in the code:**
`mcp.run(transport="http", host="0.0.0.0", port=8080)`

**For this one:**
`mcp.run(transport="stdio")`

### Steps

1.  **Install MCPHOST**

    ```bash
    go install github.com/mark3labs/mcphost@latest
    ```

2.  **Export Go to your PATH**

    ```bash
    export PATH=$PATH:$(go env GOPATH)/bin
    ```

3.  **Execute MCPHOST**

    Run `mcphost` and point it to the application and your desired Ollama model.

    ```bash
    mcphost --config /work/mcphost.json --model {your_model}
    ```
    > More info about MCPHOST can be found here: [https://github.com/mark3labs/mcphost](https://github.com/mark3labs/mcphost?tab=readme-ov-file)

4.  **Prompt Example**

    a. Execute `lithops_functionexecutor` without parameters.

    b. Run a `map` operation.

    ```python
    lithops_map(
          func="def my_function(x, y): return x + y",
          name_func="my_function",
          map_iterdata=[[1, 2], [3, 4], [5, 6]],
          tuple_list=True
    )
    ```
    c. Fetch the results using `get_result`.
