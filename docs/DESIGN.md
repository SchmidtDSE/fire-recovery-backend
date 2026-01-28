# Fire Recovery Backend - Design Patterns

This document describes the design patterns implemented in the Fire Recovery Backend and the rationale behind architectural decisions.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Command Pattern](#command-pattern)
- [Strategy Pattern](#strategy-pattern)
- [Factory Pattern](#factory-pattern)
- [Repository Pattern](#repository-pattern)
- [SOLID Principles](#solid-principles)
- [Directory Structure](#directory-structure)

---

## Architecture Overview

The Fire Recovery Backend follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                      API Layer                              │
│  (src/routers/fire_recovery.py)                            │
│  - HTTP request/response handling                          │
│  - Input validation via Pydantic                           │
│  - Background task orchestration                           │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Command Layer                            │
│  (src/commands/)                                           │
│  - Business logic encapsulation                            │
│  - Serverless-compatible operations                        │
│  - Testable in isolation                                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  Computation Layer                          │
│  (src/computation/)                                        │
│  - Spectral index calculations                             │
│  - Strategy pattern for different metrics                  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Storage Layer                            │
│  (src/core/storage/)                                       │
│  - Storage abstraction interface                           │
│  - Memory storage for temp files                           │
│  - MinIO/GCS for persistent assets                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Command Pattern

**Location:** `src/commands/`

The Command pattern is the core architectural pattern, encapsulating each business operation as a self-contained command object.

### Why Command Pattern?

1. **Serverless Compatibility** - Commands don't depend on local filesystem; all I/O goes through storage abstractions
2. **Testability** - Commands can be unit tested in isolation with mock dependencies
3. **Single Responsibility** - Each command handles exactly one business operation
4. **Consistent Interface** - All commands implement the same interface for uniform handling

### Structure

```
src/commands/
├── interfaces/
│   ├── command.py           # Abstract base Command interface
│   ├── command_context.py   # Execution context (dependencies, config)
│   └── command_result.py    # Standardized result handling
├── impl/
│   ├── fire_severity_command.py        # Fire severity analysis
│   ├── boundary_refinement_command.py  # COG cropping to boundary
│   ├── vegetation_resolve_command.py   # Vegetation impact stats
│   ├── upload_aoi_command.py          # GeoJSON/Shapefile upload
│   └── health_check_command.py        # System health verification
├── registry/
│   └── command_registry.py   # Command factory with DI
└── executor/
    └── command_executor.py   # Execution orchestration
```

### Command Interface

```python
# src/commands/interfaces/command.py
class Command(ABC):
    @abstractmethod
    async def execute(self, context: CommandContext) -> CommandResult:
        """Execute the command with given context"""
        pass

    @abstractmethod
    def get_command_name(self) -> str:
        """Return unique command identifier"""
        pass

    @abstractmethod
    def validate_context(self, context: CommandContext) -> bool:
        """Validate execution context before running"""
        pass
```

### CommandContext

The `CommandContext` encapsulates all dependencies a command needs, enabling dependency injection:

```python
# src/commands/interfaces/command_context.py
@dataclass
class CommandContext:
    job_id: str
    fire_event_name: str
    geometry: Optional[Polygon | MultiPolygon | Feature]
    storage: StorageInterface           # Injected storage abstraction
    storage_factory: StorageFactory     # For creating temp/final storage
    stac_manager: STACJSONManager       # STAC metadata operations
    index_registry: IndexRegistry       # Spectral index calculators
    computation_config: Dict[str, Any]  # Date ranges, parameters
    metadata: Dict[str, Any]            # Additional context
```

### Example: FireSeverityAnalysisCommand

```python
# src/commands/impl/fire_severity_command.py
class FireSeverityAnalysisCommand(Command):
    def get_command_name(self) -> str:
        return "fire_severity_analysis"

    def validate_context(self, context: CommandContext) -> bool:
        config = context.computation_config or {}
        return all([
            context.geometry is not None,
            config.get("prefire_date_range"),
            config.get("postfire_date_range"),
        ])

    async def execute(self, context: CommandContext) -> CommandResult:
        # 1. Fetch satellite data via STAC
        # 2. Calculate indices using IndexRegistry (Strategy pattern)
        # 3. Create COGs and upload via storage abstraction
        # 4. Create STAC metadata
        # 5. Return result with asset URLs
```

---

## Strategy Pattern

**Location:** `src/computation/`

The Strategy pattern enables interchangeable spectral index calculations.

### Why Strategy Pattern?

1. **Open/Closed Principle** - Add new indices without modifying existing code
2. **Single Responsibility** - Each calculator handles one specific metric
3. **Runtime Selection** - Choose indices dynamically based on request

### Structure

```
src/computation/
├── interfaces/
│   └── index_calculator.py   # Abstract calculator interface
├── impl/
│   └── indices/
│       ├── nbr_calculator.py     # NBR calculation
│       ├── dnbr_calculator.py    # dNBR calculation
│       ├── rdnbr_calculator.py   # RdNBR calculation
│       └── rbr_calculator.py     # RBR calculation
└── registry/
    └── index_registry.py     # Calculator factory/registry
```

### Calculator Interface

```python
# src/computation/interfaces/index_calculator.py
class IndexCalculator(ABC):
    @abstractmethod
    def calculate(
        self,
        prefire_data: np.ndarray,
        postfire_data: np.ndarray,
    ) -> np.ndarray:
        """Calculate the spectral index"""
        pass

    @abstractmethod
    def get_index_name(self) -> str:
        """Return the index identifier (e.g., 'dnbr', 'rbr')"""
        pass
```

### Index Registry

```python
# src/computation/registry/index_registry.py
class IndexRegistry:
    def __init__(self):
        self._calculators: Dict[str, IndexCalculator] = {}
        self._register_defaults()

    def _register_defaults(self):
        self.register(NBRCalculator())
        self.register(DNBRCalculator())
        self.register(RdNBRCalculator())
        self.register(RBRCalculator())

    def get_calculator(self, index_name: str) -> IndexCalculator:
        return self._calculators[index_name]

    def calculate_all(self, indices: List[str], prefire, postfire) -> Dict[str, np.ndarray]:
        return {
            name: self.get_calculator(name).calculate(prefire, postfire)
            for name in indices
        }
```

---

## Factory Pattern

**Location:** `src/core/storage/`

The Factory pattern manages storage instance creation with lifecycle-based semantics.

### Why Factory Pattern?

1. **Encapsulate Creation** - Hide complex initialization logic
2. **Lifecycle Management** - Temp vs. persistent storage have different needs
3. **Environment Flexibility** - Same code works with MinIO, GCS, or memory

### StorageFactory

```python
# src/core/storage/storage_factory.py
class StorageFactory:
    def __init__(
        self,
        temp_storage_type: str,      # "memory"
        temp_storage_config: Dict,
        final_storage_type: str,     # "minio"
        final_storage_config: Dict,
    ):
        self._temp_storage = self._create_storage(temp_storage_type, temp_storage_config)
        self._final_storage = self._create_storage(final_storage_type, final_storage_config)

    def get_temp_storage(self) -> StorageInterface:
        """Ephemeral storage for intermediate processing files"""
        return self._temp_storage

    def get_final_storage(self) -> StorageInterface:
        """Persistent storage for assets that outlive the request"""
        return self._final_storage
```

### Storage Lifecycle Semantics

| Storage Type | Purpose | Implementation | Cleanup |
|--------------|---------|----------------|---------|
| Temp Storage | Intermediate files during processing | Memory | Automatic (GC) |
| Final Storage | Persistent assets (COGs, GeoJSON) | MinIO/GCS | Manual/policy |

---

## Repository Pattern

**Location:** `src/stac/`

The Repository pattern abstracts STAC catalog operations.

### Why Repository Pattern?

1. **Data Access Abstraction** - Commands don't know how STAC items are stored
2. **Testability** - Easy to mock for unit tests
3. **Consistency** - Centralized STAC metadata creation

### STAC Managers

```python
# src/stac/stac_json_manager.py
class STACJSONManager:
    async def create_severity_item(
        self,
        fire_event_name: str,
        job_id: str,
        cog_urls: Dict[str, str],
        bbox: List[float],
        datetime_str: str,
    ) -> Dict:
        """Create and store a STAC item for fire severity COGs"""
        pass

    async def get_item_by_id(self, item_id: str) -> Optional[Dict]:
        """Retrieve a STAC item by ID"""
        pass
```

---

## Directory Structure

```
src/
├── app.py                    # FastAPI application entry point
├── routers/
│   └── fire_recovery.py      # HTTP endpoints (thin layer)
├── commands/
│   ├── interfaces/           # Command abstractions
│   ├── impl/                 # Concrete command implementations
│   ├── registry/             # Command factory
│   └── executor/             # Execution orchestration
├── computation/
│   ├── interfaces/           # Calculator abstractions
│   ├── impl/indices/         # Spectral index calculators
│   └── registry/             # Calculator factory
├── core/
│   └── storage/
│       ├── interface.py      # Storage abstraction
│       ├── memory.py         # In-memory storage
│       ├── minio.py          # S3-compatible storage
│       └── storage_factory.py
├── stac/
│   ├── stac_json_manager.py  # STAC item operations
│   └── stac_catalog_manager.py
├── models/
│   ├── requests.py           # Pydantic request models
│   └── responses.py          # Pydantic response models
├── config/
│   ├── constants.py          # Application constants
│   └── vegetation_schemas.py # Vegetation classification config
├── process/                  # Legacy processing (being migrated to commands)
└── util/                     # Shared utilities
```

---

## Key Design Decisions

### 1. Async-First Architecture

All commands are async (`async def execute`) to support:
- Non-blocking I/O for satellite data fetching
- Concurrent processing of multiple indices
- Efficient background task execution

### 2. Memory-Based Intermediate Storage

Temp files use in-memory storage instead of filesystem:
- **Why**: Serverless environments (Cloud Run) have limited/ephemeral disk
- **How**: `StorageFactory.get_temp_storage()` returns `MemoryStorage`
- **Tradeoff**: Memory pressure for large rasters (mitigated by streaming COG creation)

### 3. STAC as Asset Registry

All generated assets are registered in STAC:
- **Discovery**: Frontend finds assets via STAC queries
- **Metadata**: Spatial extent, temporal info, provenance
- **Interoperability**: Standard format for geospatial catalogs

### 4. Background Tasks for Long Operations

Processing endpoints return immediately with `job_id`:
- **Why**: Satellite data processing takes 30-120 seconds
- **How**: FastAPI `BackgroundTasks` with polling results endpoints
- **Future**: Could migrate to Cloud Tasks for better reliability
