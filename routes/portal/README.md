# Portal Routes Documentation

This directory contains all Flask blueprints for the portal (/portal_associacao) section of the application.

## Files Overview

### 1. dashboard.py
**Blueprint Name:** `dashboard`

Handles the main portal dashboard page.

**Routes:**
- `GET /portal_associacao/` - Render dashboard
- `GET /portal_associacao/dashboard` - Render dashboard (alias)

**Functions:**
- `render_dashboard()` - Renders dashboard with statistics

**Template Used:** `portal/dashboard.html`

---

### 2. users.py
**Blueprint Name:** `users`

Handles user management operations.

**Routes:**
- `GET /portal_associacao/users/` - Render users list (render_users)
- `GET /portal_associacao/users` - List users (list_and_create_users)
- `POST /portal_associacao/users` - Create new user
- `GET /portal_associacao/users/<id>` - Get user details (JSON)
- `PUT /portal_associacao/users/<id>` - Update user
- `POST /portal_associacao/users/<id>` - Update user (form)
- `DELETE /portal_associacao/users/<id>` - Delete user
- `GET /portal_associacao/users/search` - Search users (JSON)

**Features:**
- Authentication check
- Pagination (default: 10 per page)
- Full CRUD operations
- Search functionality
- JSON endpoints for modal operations

**Template Used:** `portal/users/users.html`

---

### 3. outlets.py
**Blueprint Name:** `outlets`

Handles outlet management operations.

**Routes:**
- `GET /portal_associacao/outlets/` - Render outlets list (get_outlets)
- `GET /portal_associacao/outlets` - List outlets (list_and_create_outlets)
- `POST /portal_associacao/outlets` - Create new outlet
- `GET /portal_associacao/outlets/<id>` - Get outlet details (JSON)
- `PUT /portal_associacao/outlets/<id>` - Update outlet
- `POST /portal_associacao/outlets/<id>` - Update outlet (form)
- `DELETE /portal_associacao/outlets/<id>` - Delete outlet
- `GET /portal_associacao/outlets/search` - Search outlets (JSON)

**Features:**
- Authentication check
- Pagination (default: 10 per page)
- Full CRUD operations
- Search functionality
- JSON endpoints for modal operations

**Template Used:** `portal/outlets/outlets.html`

---

### 4. assets.py
**Blueprint Name:** `assets`

Handles asset management operations.

**Routes:**
- `GET /portal_associacao/assets/` - List assets (list_assets)
- `GET /portal_associacao/assets` - List assets (list_and_create_assets)
- `POST /portal_associacao/assets` - Create new asset
- `GET /portal_associacao/assets/<id>` - Get asset details (JSON)
- `PUT /portal_associacao/assets/<id>` - Update asset
- `POST /portal_associacao/assets/<id>` - Update asset (form)
- `DELETE /portal_associacao/assets/<id>` - Delete asset
- `GET /portal_associacao/assets/search` - Search assets (JSON)

**Features:**
- Authentication check
- Pagination (default: 10 per page)
- Full CRUD operations
- Search functionality
- JSON endpoints for modal operations

**Template Used:** `portal/assets/assets.html`

---

### 5. smartdevices.py
**Blueprint Name:** `smart_devices`

Handles smart device management operations.

**Routes:**
- `GET /portal_associacao/smartdevices/` - List devices (get_smart_devices)
- `GET /portal_associacao/smartdevices` - List devices (list_and_create_smartdevices)
- `POST /portal_associacao/smartdevices` - Create new device
- `GET /portal_associacao/smartdevices/<id>` - Get device details (JSON)
- `PUT /portal_associacao/smartdevices/<id>` - Update device
- `POST /portal_associacao/smartdevices/<id>` - Update device (form)
- `DELETE /portal_associacao/smartdevices/<id>` - Delete device
- `GET /portal_associacao/smartdevices/search` - Search devices (JSON)

**Features:**
- Authentication check
- Pagination (default: 10 per page)
- Full CRUD operations
- Search functionality
- JSON endpoints for modal operations

**Template Used:** `portal/smartdevices/smartdevices.html`

---

### 6. tracking.py
**Blueprint Name:** `tracking`

Handles smart device tracking and mapping.

**Routes:**
- `GET /portal_associacao/tracking/` - Render tracking map page
- `GET /portal_associacao/tracking/api/devices` - Get devices for map (JSON API)

**Features:**
- Authentication check
- Pagination support (default: 20 per page)
- Search functionality
- Location data in responses
- Device marker clustering support

**Query Parameters for API:**
- `page` - Page number (default: 1)
- `per_page` - Items per page (default: 20)
- `search` - Search term (searches serial_number, mac_address, outlet, linked_asset, city)

**Response Format:**
```json
{
  "data": [
    {
      "id": 1,
      "serial_number": "...",
      "mac_address": "...",
      "outlet": "...",
      "latitude": 0.0,
      "longitude": 0.0,
      "has_location": true,
      ...
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 20,
    "total": 100,
    "pages": 5,
    "has_prev": false,
    "has_next": true
  }
}
```

**Template Used:** `portal/tracking/tracking.html`

---

### 7. auth.py
**Blueprint Name:** `portal_auth`

Handles authentication for the portal.

**Routes:**
- `GET /portal_associacao/auth/logout` - Logout user
- `POST /portal_associacao/auth/logout` - Logout user

**Functions:**
- `logout()` - Clear session and redirect to login

---

## Common Features Across All Routes

### Authentication
All routes (except logout) require user authentication. They check `session["user"]` and redirect to login page if not authenticated.

```python
def require_authentication():
    """Check if user is authenticated"""
    user = session.get("user")
    if not user:
        return None
    return user
```

### Pagination
List endpoints support pagination with `page` query parameter:
```
GET /portal_associacao/users?page=2
```

Default items per page: 10 (can be overridden)

### Search
Search endpoints accept `q` query parameter:
```
GET /portal_associacao/users/search?q=john
```

Returns JSON array with matching items (limit: 20 results)

### Error Handling
All routes include:
- Try-catch blocks for exception handling
- Database rollback on errors
- Flash messages for user feedback
- Proper HTTP status codes

### Database Operations
- All database operations use SQLAlchemy ORM
- Proper session management with get_session()
- Timestamps: `created_on`, `modified_on`
- User tracking: `created_by`, `modified_by`

### HTTP Method Override
Forms can use `_method` hidden input to simulate PUT/DELETE:
```html
<form method="POST">
  <input type="hidden" name="_method" value="PUT">
  ...
</form>
```

This is handled by the middleware in app.py.

---

## Usage Examples

### Create a User
```bash
POST /portal_associacao/users
Content-Type: application/x-www-form-urlencoded

first_name=John&last_name=Doe&email=john@example.com&upn=john.doe
```

### Update a User
```bash
POST /portal_associacao/users/123
Content-Type: application/x-www-form-urlencoded

_method=PUT&first_name=John&last_name=Doe&email=john.new@example.com
```

### Delete a User
```bash
POST /portal_associacao/users/123
Content-Type: application/x-www-form-urlencoded

_method=DELETE
```

### Get User Details (JSON)
```bash
GET /portal_associacao/users/123
```

Response:
```json
{
  "id": 123,
  "first_name": "John",
  "last_name": "Doe",
  "email": "john@example.com",
  ...
}
```

### Search Users
```bash
GET /portal_associacao/users/search?q=john
```

Response:
```json
[
  {
    "id": 123,
    "first_name": "John",
    "last_name": "Doe",
    "email": "john@example.com",
    ...
  }
]
```

---

## Template Integration

All templates expect certain route names to be available via `url_for()`:

### Dashboard Template
- `url_for('users.render_users')` → `/portal_associacao/users/`
- `url_for('outlets.get_outlets')` → `/portal_associacao/outlets/`
- `url_for('assets.list_assets')` → `/portal_associacao/assets/`
- `url_for('smart_devices.get_smart_devices')` → `/portal_associacao/smartdevices/`

### List Templates
- `url_for('users.list_and_create_users', page=X)` → Pagination
- `url_for('outlets.list_and_create_outlets', page=X)` → Pagination
- `url_for('assets.list_and_create_assets', page=X)` → Pagination
- `url_for('smartdevices.list_and_create_smartdevices', page=X)` → Pagination

### Authentication
- `url_for('portal_auth.logout')` → `/portal_associacao/auth/logout`

---

## Status Codes

All routes return appropriate HTTP status codes:

- `200` - Success
- `201` - Created (implicit in redirects)
- `302` - Redirect
- `401` - Unauthorized (not authenticated)
- `404` - Not found
- `500` - Internal server error

---

## Security Considerations

1. **Authentication:** All routes check session before processing
2. **CSRF:** Uses Flask session for token management
3. **SQL Injection:** Uses SQLAlchemy ORM (parameterized queries)
4. **XSS:** Templates use Jinja2 auto-escaping
5. **Method Override:** Only allows PUT/DELETE/PATCH via _method parameter

---

## Testing

To test the routes:

1. Ensure database is initialized
2. Login to set session["user"]
3. Make requests to the portal routes
4. Check for proper redirects and responses

---

## Troubleshooting

### 401 Unauthorized
- User is not authenticated
- Session["user"] is not set
- Try logging in again

### 404 Not Found
- Route doesn't exist
- Blueprint not registered
- Check URL format and endpoint name

### Database Errors
- Check database connection
- Verify models are correctly defined
- Check for missing columns in database

### Template Not Found
- Verify template path
- Check template directory structure
- Ensure template file exists
