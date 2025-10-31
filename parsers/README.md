# olmOCR Parsers

This directory contains different parsing providers and a unified interface to run them.

## Structure

```
parsers/
├── run_parser.py          # Unified parser runner
├── llamaparse/
│   ├── parse.py          # LlamaParse implementation
│   └── configs/          # LlamaParse configs
│       ├── agent_openai_gpt41mini.json
│       ├── agent_haiku45.json
│       ├── agent_sonnet45.json
│       └── fast_mode.json
├── reducto/
│   ├── parse.py          # Reducto implementation
│   └── configs/          # Reducto configs (if any)
└── <other-parsers>/
    ├── parse.py
    └── configs/
```

## Usage

### Quick Start

From the project root:

```bash
# List available configs for a parser
./parse llamaparse --list-configs dummy_input

# Parse with a specific config (just use the config name, not the full path)
./parse llamaparse input_dir --config agent_openai_gpt41mini

# Parse with fast mode
./parse llamaparse input_dir --config fast_mode --output-suffix my_run

# Pass additional arguments to the parser (e.g., recursive search)
./parse llamaparse input_dir --config agent_sonnet45 -r --extensions .pdf .docx
```

### Direct Usage

You can also run the parser script directly:

```bash
python parsers/run_parser.py llamaparse input_dir --config agent_openai_gpt41mini
```

### Available Parsers

- **llamaparse**: LlamaParse parsing service
- **reducto**: Reducto parsing service

To see all available parsers, run:
```bash
./parse --help
```

### Config System

Each parser has its own `configs/` directory containing JSON configuration files. When you specify `--config <name>`, the system automatically looks in `parsers/<parser>/configs/<name>.json`.

**Important**: You only need to specify the config name (e.g., `agent_openai_gpt41mini`), not the full path or `.json` extension.

### Adding a New Parser

1. Create a new directory: `parsers/<parser-name>/`
2. Add a `parse.py` file with your parser implementation
3. (Optional) Create a `configs/` directory with JSON config files
4. Add `__init__.py` file
5. Ensure `parse.py` accepts `--config` argument with a path to config file
6. The parser will automatically appear in `./parse --help`

### Parser-Specific Arguments

The unified runner passes all unknown arguments to the specific parser script. For example:

```bash
# These arguments are passed directly to llamaparse/parse.py
./parse llamaparse input_dir --config fast_mode --extensions .pdf .docx -r --output-suffix my_run
```

Common arguments that each parser should support:
- `input`: Input file or directory (positional argument)
- `--config`: Path to config file
- `--extensions`: File extensions to process
- `--output-suffix`: Suffix for output directory
- `-r, --recursive`: Recursive directory search

## Examples

### Example 1: Parse with OpenAI GPT-4 Mini Agent
```bash
./parse llamaparse ./documents --config agent_openai_gpt41mini
```

### Example 2: Parse recursively with Haiku 4.5
```bash
./parse llamaparse ./documents --config agent_haiku45 -r --extensions .pdf .docx
```

### Example 3: Fast mode with custom output
```bash
./parse llamaparse ./documents --config fast_mode --output-suffix fast_test
```

### Example 4: Use Reducto parser
```bash
./parse reducto ./documents
```
