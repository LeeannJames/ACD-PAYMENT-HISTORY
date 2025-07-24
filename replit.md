# Payment Data Scraper

## Overview

This is a Flask-based web application that scrapes specific payment data columns (Date, Pen, Principal, CBU, CBU withdraw, Collector) from loan information web pages and provides the extracted data in a downloadable Excel format. The application uses BeautifulSoup for web scraping, pandas for data manipulation, and Bootstrap for the frontend interface.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

The application follows a simple Flask web application architecture with a modular design:

- **Frontend**: Bootstrap-based responsive web interface with custom styling
- **Backend**: Flask web server with session management
- **Data Processing**: BeautifulSoup for HTML parsing and pandas for data manipulation
- **File Handling**: Temporary file generation for Excel downloads

## Key Components

### 1. Web Application (app.py)
- **Flask Application**: Main web server handling routes and requests
- **Session Management**: Uses Flask sessions with configurable secret key
- **Error Handling**: Flash messaging system for user feedback
- **URL Validation**: Built-in URL format validation using urlparse

### 2. Web Scraper (scraper.py)
- **PaymentDataScraper Class**: Handles web scraping operations for specific columns
- **Target Columns**: Date, Receipt No, Principal, Pen, CBU, CBU withdraw, Collector
- **Calculated Columns**: 
  - Principal_PassBook & Principal_Variance (for Principal + Pen calculations)
  - CBU_PassBook & CBU_Variance (for CBU + CBU withdraw calculations)
- **Smart Table Detection**: Identifies payment transaction tables specifically
- **Column Mapping**: Maps various header formats to target columns including Receipt No
- **HTTP Session Management**: Persistent session with realistic user-agent headers
- **Error Handling**: Comprehensive logging and exception handling

### 3. Frontend Interface
- **Templates**: Jinja2 templates with Bootstrap styling
  - `index.html`: Main URL input form
  - `preview.html`: Data preview and download interface
- **Static Assets**: 
  - Custom CSS for enhanced styling and animations
  - JavaScript for form validation and interactive features

## Data Flow

1. **User Input**: User submits a URL through the web form
2. **URL Validation**: Backend validates URL format and accessibility
3. **Web Scraping**: PaymentDataScraper extracts payment data using multiple methods
4. **Data Processing**: Extracted data is structured and stored in session
5. **Preview Display**: User can preview extracted data in a table format
6. **Excel Generation**: Data is converted to Excel format using pandas
7. **File Download**: Temporary Excel file is served for download

## External Dependencies

### Python Packages
- **Flask**: Web framework for the application
- **BeautifulSoup4**: HTML parsing and web scraping
- **requests**: HTTP client for web requests
- **pandas**: Data manipulation and Excel file generation
- **werkzeug**: WSGI utilities and proxy handling

### Frontend Libraries
- **Bootstrap**: CSS framework with dark theme
- **Font Awesome**: Icon library
- **Custom CSS/JS**: Enhanced user experience and interactions

## Deployment Strategy

The application is configured for flexible deployment:

- **Development**: Direct Flask development server (main.py)
- **Production Ready**: ProxyFix middleware for reverse proxy compatibility
- **Environment Configuration**: Environment-based secret key management
- **Session Management**: Configurable session handling for user data persistence
- **Logging**: Comprehensive logging system for debugging and monitoring

### Key Deployment Considerations
- Secret key should be set via SESSION_SECRET environment variable
- Application runs on host 0.0.0.0:5000 by default
- Debug mode enabled for development environments
- Proxy-aware configuration for deployment behind reverse proxies

### Scalability Notes
- Stateless design except for temporary session data
- Temporary file cleanup handled automatically
- Memory-efficient streaming for large datasets
- Session-based data storage for multi-step workflows