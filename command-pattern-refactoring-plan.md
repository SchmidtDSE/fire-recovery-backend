# Command Pattern Refactoring Plan

## Executive Summary

This document outlines the implementation of the Command pattern to separate API concerns from business logic in the fire recovery backend. The refactoring addresses serverless compatibility issues, improves testability, and creates clear separation of concerns while adhering to SOLID principles.

## Current State Analysis

### Problems Identified

#### 1. Mixed Concerns in API Layer
- **Location**: `src/routers/fire_recovery.py:179-241`
- **Issue**: HTTP handlers contain business logic, making them difficult to test and maintain
- **Impact**: Tight coupling between API and domain logic

#### 2. Local I/O Dependencies
- **Location**: `src/process/resolve_veg.py:18-48`
- **Issue**: Direct `tempfile` usage prevents serverless deployment
- **Impact**: Cannot run in distributed/serverless environments

#### 3. Hardcoded File System Operations
- **Location**: `src/process/spectral_indices.py:125-134`
- **Issue**: Local filesystem paths (`tmp/{job_id}`) hardcoded
- **Impact**: Not compatible with cloud-native architectures

#### 4. Overloaded Process Modules
- **Location**: `src/process/*`
- **Issue**: Single modules handling multiple responsibilities (I/O, computation, orchestration)
- **Impact**: Violates Single Responsibility Principle

### Current Architecture Strengths

✅ **Strategy Pattern**: Well-implemented for spectral indices calculation
✅ **STAC Integration**: Clean separation using pystac library
✅ **Storage Abstraction**: Factory pattern for memory/minio storage
✅ **Dependency Injection**: IndexRegistry properly manages calculator dependencies

## Design Pattern Selection: Command Pattern

### Why Command Pattern?

The Command pattern is ideal for this refactoring because it:

1. **Encapsulates Business Operations**: Each fire recovery operation becomes a self-contained command
2. **Enables Undo/Redo**: Future requirement for operation rollback
3. **Supports Queuing**: Background task execution fits naturally
4. **Promotes Testability**: Commands can be unit tested in isolation
5. **Facilitates Logging**: Centralized execution monitoring
6. **Decouples Invoker from Receiver**: API layer doesn't need to know implementation details

### SOLID Principles Compliance

#### Single Responsibility Principle (SRP)
- Each command handles exactly one business operation
- API controllers only handle HTTP concerns
- Storage classes only handle data persistence

#### Open/Closed Principle (OCP)
- New operations can be added as new commands without modifying existing code
- Command registry enables runtime command discovery

#### Liskov Substitution Principle (LSP)
- All commands implement the same `Command` interface
- Commands are interchangeable through the common interface

#### Interface Segregation Principle (ISP)
- Commands only depend on interfaces they actually use
- Separate interfaces for different types of dependencies

#### Dependency Inversion Principle (DIP)
- Commands depend on abstractions (storage interfaces, computation interfaces)
- High-level modules don't depend on low-level implementation details

## Architecture Design

### Directory Structure

```
src/commands/
├── __init__.py
├── interfaces/
│   ├── __init__.py
│   ├── command.py              # Base Command interface
│   ├── command_context.py      # Execution context
│   └── command_handler.py      # Handler abstraction
├── impl/
│   ├── __init__.py
│   ├── fire_severity_command.py
│   ├── boundary_refinement_command.py
│   └── vegetation_analysis_command.py
├── registry/
│   ├── __init__.py
│   └── command_registry.py     # Command factory
└── executor/
    ├── __init__.py
    └── command_executor.py     # Orchestration
```

### Core Interfaces

#### Command Interface
```python
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
    """Validate execution context"""
    pass
```

#### CommandContext
```python
class CommandContext:
    """Encapsulates all data needed for command execution"""
    job_id: str
    fire_event_name: str
    geometry: Union[Polygon, Dict]
    storage: StorageInterface
    stac_manager: STACJSONManager
    computation_config: Dict[str, Any]
```

### Command Implementations

#### 1. FireSeverityAnalysisCommand

**Responsibility**: Process remote sensing data to calculate fire severity indices

**Current Logic Location**: `src/process/spectral_indices.py` + `src/routers/fire_recovery.py:179-241`

**Dependencies**:
- `IndexRegistry` (existing strategy pattern)
- `StacEndpointHandler` (existing)
- `StorageInterface` (abstraction)
- `STACJSONManager` (existing)

**Execution Flow**:
1. Validate input parameters (geometry, date ranges)
2. Fetch remote sensing data via STAC
3. Calculate indices using strategy pattern
4. Store results via storage abstraction
5. Create STAC metadata
6. Return command result with asset URLs

#### 2. BoundaryRefinementCommand

**Responsibility**: Refine fire boundaries and crop existing COGs

**Current Logic Location**: `src/routers/fire_recovery.py:311-374`

**Dependencies**:
- `StorageInterface` (for COG processing)
- `STACJSONManager` (for metadata)
- Geometry processing utilities

**Execution Flow**:
1. Validate refined boundary geometry
2. Retrieve original COG from storage
3. Crop COG with refined boundary
4. Store refined COG via storage abstraction
5. Update STAC metadata
6. Return refined asset URLs

#### 3. VegetationAnalysisCommand

**Responsibility**: Analyze vegetation impact from fire severity data

**Current Logic Location**: `src/process/resolve_veg.py`

**Dependencies**:
- `StorageInterface` (no tempfiles)
- Vegetation data processing utilities
- Statistical analysis components

**Execution Flow**:
1. Download vegetation and fire data to memory
2. Perform zonal statistics calculation
3. Generate CSV and JSON reports
4. Store results via storage abstraction
5. Create STAC metadata for analysis
6. Return analysis result URLs

### Command Registry Pattern

#### Purpose
- **Factory Pattern**: Creates appropriate commands based on operation type
- **Dependency Injection**: Injects required dependencies into commands
- **Configuration Management**: Centralized command configuration

#### Implementation
```python
class CommandRegistry:
    def __init__(
        self,
        storage_factory: StorageFactory,
        stac_manager: STACJSONManager,
        index_registry: IndexRegistry
    ):
        self._storage_factory = storage_factory
        self._stac_manager = stac_manager
        self._index_registry = index_registry
        self._commands: Dict[str, Type[Command]] = {}
        self._setup_commands()
    
    def create_command(self, command_type: str, context: CommandContext) -> Command:
        """Create command instance with proper dependency injection"""
        pass
```

### Command Executor Pattern

#### Purpose
- **Orchestration**: Manages command execution lifecycle
- **Error Handling**: Centralized exception management
- **Logging**: Consistent execution monitoring
- **Retry Logic**: Handles transient failures

#### Features
- Async execution support
- Progress tracking
- Resource cleanup
- Execution metrics

## Implementation Phases

### Phase 1: Command Infrastructure (Days 1-2)

**Deliverables**:
- [ ] Base `Command` interface
- [ ] `CommandContext` data structure
- [ ] `CommandResult` response model
- [ ] `CommandRegistry` factory
- [ ] `CommandExecutor` orchestration
- [ ] Unit tests for infrastructure

**Success Criteria**:
- All interfaces compile without errors
- Mock command can be registered and executed
- Error handling works correctly

### Phase 2: Business Logic Commands (Days 3-5)

**Deliverables**:
- [ ] `FireSeverityAnalysisCommand` implementation
- [ ] `BoundaryRefinementCommand` implementation
- [ ] `VegetationAnalysisCommand` implementation
- [ ] Integration with existing strategy patterns
- [ ] Comprehensive unit tests

**Success Criteria**:
- Each command passes unit tests in isolation
- Commands properly use storage abstraction
- No tempfile dependencies remain
- Logging covers all execution paths

### Phase 3: API Layer Refactoring (Days 6-7)

**Deliverables**:
- [ ] Refactored HTTP handlers to use commands
- [ ] Request validation moved to appropriate layer
- [ ] Response mapping from command results
- [ ] Integration tests

**Success Criteria**:
- All existing API endpoints work unchanged
- API handlers contain only HTTP concerns
- Business logic fully encapsulated in commands

### Phase 4: Serverless Compatibility (Day 8)

**Deliverables**:
- [ ] Remove all tempfile usage
- [ ] Eliminate local filesystem dependencies
- [ ] Memory-based intermediate storage
- [ ] Cloud storage integration tests

**Success Criteria**:
- No local I/O operations in business logic
- Commands work in serverless environments
- Storage abstraction handles all persistence

## Benefits of This Approach

### 1. Separation of Concerns
- **API Layer**: Only handles HTTP requests/responses, validation, serialization
- **Command Layer**: Contains pure business logic without infrastructure concerns
- **Storage Layer**: Abstracts persistence without business knowledge

### 2. Testability Improvements
- **Unit Testing**: Commands can be tested independently with mock dependencies
- **Integration Testing**: Command executor can be tested with real storage
- **API Testing**: HTTP layer tests focus on request/response handling

### 3. Serverless Compatibility
- **No Local I/O**: All file operations use storage abstractions
- **Stateless Execution**: Commands don't rely on local filesystem
- **Cloud Native**: Works in containerized and serverless environments

### 4. Maintainability
- **Single Responsibility**: Each component has clear, focused purpose
- **Dependency Injection**: Easy to swap implementations for testing/deployment
- **Consistent Patterns**: All business operations follow same structure

### 5. Extensibility
- **New Operations**: Add new commands without modifying existing code
- **Cross-Cutting Concerns**: Logging, metrics, retry logic handled centrally
- **Plugin Architecture**: Commands can be loaded dynamically

## Risk Mitigation

### 1. Performance Concerns
- **Mitigation**: Command pattern adds minimal overhead
- **Monitoring**: Execution metrics will track performance impact
- **Optimization**: Storage abstraction enables caching strategies

### 2. Complexity Increase
- **Mitigation**: Clear interfaces and documentation
- **Training**: Team education on command pattern benefits
- **Gradual Adoption**: Phased implementation reduces risk

### 3. Backward Compatibility
- **Mitigation**: API endpoints maintain same external interface
- **Testing**: Comprehensive integration tests ensure compatibility
- **Rollback Plan**: Feature flags enable quick reversal if needed

## Success Metrics

### Technical Metrics
- [ ] 0 tempfile usages in business logic
- [ ] 100% unit test coverage for commands
- [ ] <50ms additional latency per command
- [ ] 0 local filesystem dependencies

### Quality Metrics
- [ ] All SOLID principles violations resolved
- [ ] Code complexity reduced by 30%
- [ ] API handler line count reduced by 50%
- [ ] Business logic fully testable in isolation

### Operational Metrics
- [ ] Commands deployable in serverless environment
- [ ] Centralized logging covers all execution paths
- [ ] Error handling provides actionable debugging information
- [ ] Resource cleanup prevents memory leaks

## Conclusion

The Command pattern refactoring addresses the core architectural issues in the fire recovery backend while maintaining backward compatibility. By separating API concerns from business logic and eliminating local I/O dependencies, the system becomes more testable, maintainable, and suitable for cloud-native deployment.

The phased implementation approach minimizes risk while delivering incremental value. The existing strategy patterns for index calculation and STAC management integrate seamlessly with the new command architecture, preserving previous refactoring investments.

This design positions the codebase for future enhancements such as operation queuing, distributed processing, and advanced monitoring capabilities while adhering to software engineering best practices.