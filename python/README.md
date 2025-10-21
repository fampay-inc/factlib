# FactLib Python Client

A Python library for reliable event emission using the Outbox Pattern with PostgreSQL WAL and Django integration.

## Overview

FactLib Python provides a Django-integrated client for emitting events that are guaranteed to be delivered via PostgreSQL's logical replication stream. This ensures atomicity between your business transactions and event emission - if your database transaction fails, no events are emitted; if your database transaction succeeds, events are guaranteed to be processed.

## Installation

```bash
pip install factlib
```

Or with Poetry:

```bash
poetry add factlib
```

## Quick Start

### 1. Django Settings Configuration

Add FactLib to your Django settings:

```python
# settings.py
INSTALLED_APPS = [
    # ... your other apps
    'factlib',
]

# FactLib configuration
FACTLIB_PREFIX = "your-service-name"  # Used as routing prefix for events
```

### 2. Basic Usage

```python
from factlib import Fact
from django.db import transaction

def create_user(user_data):
    with transaction.atomic():
        # Your business logic
        user = User.objects.create(**user_data)
        
        # Emit event atomically within the same transaction
        fact = Fact(
            aggregate_type="user",
            aggregate_id=str(user.id),
            event_type="user.created",
            payload=json.dumps(user_data).encode('utf-8'),
            metadata={
                "version": "1.0",
                "source": "user-service"
            }
        )
        fact.emit()
        
    return user
```

## API Reference

### `Fact` Class

The main class for creating and emitting events.

#### Constructor

```python
Fact(
    aggregate_type: str,
    aggregate_id: str,
    event_type: str,
    payload: bytes,
    metadata: Optional[Dict[str, str]] = None,
    prefix: str = None
)
```

**Parameters:**
- `aggregate_type` (str): The type of aggregate (e.g., "user", "order", "payment")
- `aggregate_id` (str): Unique identifier for the aggregate instance
- `event_type` (str): The type of event (e.g., "user.created", "order.updated")
- `payload` (bytes): The event data as bytes (usually JSON-encoded)
- `metadata` (Optional[Dict[str, str]]): Additional metadata key-value pairs
- `prefix` (Optional[str]): Override the default FACTLIB_PREFIX for this event

#### Methods

##### `emit()`
Emits the event using PostgreSQL's `pg_logical_emit_message` function.

```python
fact.emit()
```

**Raises:**
- `RuntimeError`: If the emission fails

##### `with_prefix(prefix: str)`
Sets a custom prefix for the event and returns self for method chaining.

```python
fact.with_prefix("custom-service").emit()
```

## Advanced Usage

### Custom Prefix per Event

```python
# Override default prefix for specific events
fact = Fact(
    aggregate_type="user",
    aggregate_id="123",
    event_type="user.migrated",
    payload=b'{"id": 123}',
    prefix="migration-service"
)
fact.emit()

# Or use method chaining
Fact("user", "123", "user.updated", payload).with_prefix("admin-service").emit()
```

### Complex Metadata

```python
fact = Fact(
    aggregate_type="order",
    aggregate_id="order-456",
    event_type="order.completed",
    payload=json.dumps(order_data).encode('utf-8'),
    metadata={
        "correlation_id": request_id,
        "user_id": str(user.id),
        "version": "2.1",
        "source_system": "checkout-service",
        "event_version": "1"
    }
)
fact.emit()
```

### Integration with Django Models

```python
from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from factlib import Fact

class Order(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    user_id = models.UUIDField()
    status = models.CharField(max_length=20)
    total_amount = models.DecimalField(max_digits=10, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)

@receiver(post_save, sender=Order)
def order_created_handler(sender, instance, created, **kwargs):
    if created:
        with transaction.atomic():
            fact = Fact(
                aggregate_type="order",
                aggregate_id=str(instance.id),
                event_type="order.created",
                payload=json.dumps({
                    "order_id": str(instance.id),
                    "user_id": str(instance.user_id),
                    "total_amount": str(instance.total_amount),
                    "status": instance.status,
                    "created_at": instance.created_at.isoformat()
                }).encode('utf-8'),
                metadata={
                    "version": "1.0",
                    "source": "order-service"
                }
            )
            fact.emit()
```

### Distributed Tracing Integration

FactLib automatically integrates with Sentry for distributed tracing:

```python
import sentry_sdk

# Sentry tracing context is automatically captured
with sentry_sdk.start_transaction(op="user_creation", name="create_user"):
    fact = Fact(
        aggregate_type="user",
        aggregate_id="123",
        event_type="user.created",
        payload=b'{"id": 123}'
    )
    fact.emit()  # Trace info automatically included
```

## Event Processing Pipeline

When you emit a Fact:

1. **Atomic Emission**: The event is written to PostgreSQL's WAL within your transaction
2. **WAL Streaming**: OwlPost (the Go consumer service) reads from the WAL stream
3. **Kafka Production**: Events are published to Kafka topics based on aggregate type
4. **Downstream Processing**: Other services consume events from Kafka

```
Django App → PostgreSQL WAL → OwlPost → Kafka → Downstream Services
     ↑                                              ↓
  [Atomic Transaction]                        [Event Consumers]
```

## Error Handling

```python
from factlib import Fact
from django.db import transaction

def create_user_with_error_handling(user_data):
    try:
        with transaction.atomic():
            user = User.objects.create(**user_data)
            
            fact = Fact(
                aggregate_type="user",
                aggregate_id=str(user.id),
                event_type="user.created",
                payload=json.dumps(user_data).encode('utf-8')
            )
            
            try:
                fact.emit()
            except RuntimeError as e:
                logger.error(f"Failed to emit user.created event: {e}")
                # Transaction will be rolled back automatically
                raise
                
    except Exception as e:
        logger.error(f"User creation failed: {e}")
        raise
```

## Configuration

### Required Settings

```python
# settings.py
FACTLIB_PREFIX = "your-service-name"  # Required: Used for event routing
```

### Database Requirements

- PostgreSQL with logical replication enabled
- Connection with replication privileges
- WAL level set to 'logical'

### Dependencies

The library requires:
- Django 2.2.5+
- PostgreSQL database backend
- protobuf 4.23.4
- grpcio ^1.56.0
- sentry-sdk 1.38.0 (for tracing)

## Development

### Setting up for Development

```bash
# Clone the repository
git clone <repo-url>
cd factlib/python

# Install dependencies
poetry install

# Run tests
poetry run pytest

# Pre-commit hooks
poetry run pre-commit install
```

### Testing

```python
# Example test
from django.test import TestCase, TransactionTestCase
from factlib import Fact

class FactEmissionTest(TransactionTestCase):
    def test_fact_emission(self):
        fact = Fact(
            aggregate_type="test",
            aggregate_id="123",
            event_type="test.created",
            payload=b'{"test": true}'
        )
        
        # This should not raise an exception
        fact.emit()
```

## Best Practices

1. **Always use transactions**: Emit facts within database transactions to ensure atomicity
2. **Meaningful aggregate types**: Use clear, consistent aggregate type names
3. **Structured payloads**: Use JSON for payload structure and consistency
4. **Metadata for context**: Include correlation IDs, versions, and source information
5. **Error handling**: Always handle emission failures appropriately
6. **Idempotent events**: Design events to be safely reprocessed

## Troubleshooting

### Common Issues

**"Failed to emit event" RuntimeError**
- Check PostgreSQL connection and permissions
- Ensure logical replication is enabled
- Verify `pg_logical_emit_message` function is available

**Events not appearing in Kafka**
- Verify OwlPost consumer service is running
- Check WAL replication slot is active
- Confirm Kafka connectivity from OwlPost

**Missing trace information**
- Ensure Sentry SDK is properly configured
- Check that traces are being started before fact emission

## Related

- [FactLib Go Documentation](../README.md) - Main project documentation
- [OwlPost Consumer](../cmd/owlpost/) - The Go service that processes events
- [PostgreSQL WAL Documentation](https://www.postgresql.org/docs/current/logical-replication.html)
