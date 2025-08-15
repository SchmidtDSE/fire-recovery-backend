Do not suggest 'hacky' or 'workaround' solutions. Avoid using overly complex or convoluted methods that could lead to confusion or maintenance challenges in the future.

Wherever possible, provide solutions that adhere to SOLID object-oriented principles, ensuring that the code is modular, reusable, and easy to understand.

SOLID stands for:
    S - Single-responsibility Principle
    O - Open-closed Principle
    L - Liskov Substitution Principle
    I - Interface Segregation Principle
    D - Dependency Inversion Principle

When suggesting a solution or a plan, focus on adhering to a known design pattern, such as:
- Creational Patterns
    - Singleton
    - Factory Method
    - Abstract Factory
    - Builder
    - Prototype
- Structural Patterns
    - Adapter
    - Bridge
    - Composite
    - Decorator
    - Facade
    - Flyweight
    - Proxy
- Behavioral Patterns
    - Chain of Responsibility
    - Command
    - Interpreter
    - Iterator
    - Mediator
    - Memento
    - Observer
    - State
    - Strategy
    - Template Method
    - Visitor

When discussing design patterns, ensure that the user understands the pattern's purpose and how it applies to their specific problem. Provide examples where appropriate, but avoid overwhelming the user with unnecessary complexity.

If a solution appears to violate one of these SOLID principles, explain why it does so and why it might or might not be acceptable in the given context. The user will do their best to adhere to these principles themselves (both in prompting and code) but might request a solution that violates them. In such cases, help the user understand the implications of such a decision and suggest alternatives that align better with these principles, but do not insist on them if the user has a specific reason for their request.

If a 'magic number' or a configurable is needed, find an appropriate place to define it, such as a constant or configuration file, rather than hardcoding it directly into the code. 

Do not default to generating code before fully understanding the problem. Always clarify requirements and constraints before proceeding with a solution, especially when the user seeks to understand the problem and solution rather than just the code itself. When the user wants a solution, first provide a high-level overview of the approach, ask for confirmation, and then proceed with the implementation details.

This codebase's parent organization values 'cockroach engineering' principles. Wherever possible, avoid adding new dependencies or libraries unless absolutely necessary. If a solution can be achieved with existing code or libraries, prefer that approach to minimize complexity and maintainability issues.

Whenever reasonable, add logging statements to the code to help with debugging and monitoring, particularly in areas where exceptions might be raised or where the flow of execution is critical. Use a consistent logging format and level (e.g., INFO, DEBUG, ERROR) to ensure clarity and ease of use. Be careful when using INFO or DEBUG levels, as they can lead to excessive logging in production environments. Use ERROR level for exceptions and critical issues that need immediate attention.

When suggesting code, ensure it is:

NEVER do the following:
- Suggest an import or library in the body of a function or method - instead suggest it at the top of the file.
- Use a placeholder solution (such as instantiating a dummy variable in place of a 'real' one) due to lack of information or the necessary refactor being out of scope of the original question, if so, instead explain what would need to be done to implement the solution properly.