import os
import logging
from flask import Flask, render_template, request, jsonify, send_file, flash, redirect, url_for, session
from werkzeug.middleware.proxy_fix import ProxyFix
import pandas as pd
from scraper import PaymentDataScraper
import tempfile
import uuid
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Create the app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Initialize scraper
scraper = PaymentDataScraper()

@app.route('/')
def index():
    """Main page with URL input form."""
    return render_template('index.html')

@app.route('/scrape', methods=['POST'])
def scrape_url():
    """Handle URL scraping request."""
    url = request.form.get('url', '').strip()
    
    if not url:
        flash('Please enter a valid URL', 'error')
        return redirect(url_for('index'))
    
    # Validate URL format
    try:
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            flash('Please enter a valid URL with http:// or https://', 'error')
            return redirect(url_for('index'))
    except Exception:
        flash('Invalid URL format', 'error')
        return redirect(url_for('index'))
    
    try:
        # Scrape the data
        app.logger.info(f"Starting scrape for URL: {url}")
        data = scraper.scrape_payment_data(url)
        
        if not data:
            flash('No payment data found on the specified page', 'warning')
            return redirect(url_for('index'))
        
        # Store data in session for preview
        session_id = str(uuid.uuid4())
        session[f'scraped_data_{session_id}'] = {
            'data': data,
            'url': url,
            'columns': list(data[0].keys()) if data else []
        }
        
        app.logger.info(f"Successfully scraped {len(data)} records")
        return redirect(url_for('preview', session_id=session_id))
        
    except Exception as e:
        app.logger.error(f"Error scraping URL {url}: {str(e)}")
        flash(f'Error scraping data: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/preview/<session_id>')
def preview(session_id):
    """Preview scraped data before download."""
    data_key = f'scraped_data_{session_id}'
    
    if data_key not in session:
        flash('Session expired or invalid. Please scrape again.', 'error')
        return redirect(url_for('index'))
    
    scraped_info = session[data_key]
    data = scraped_info['data']
    url = scraped_info['url']
    columns = scraped_info['columns']
    
    return render_template('preview.html', 
                         data=data, 
                         url=url, 
                         columns=columns,
                         session_id=session_id,
                         total_records=len(data))

@app.route('/update_data/<session_id>', methods=['POST'])
def update_data(session_id):
    """Update PassBook and Variance data from frontend."""
    data_key = f'scraped_data_{session_id}'
    
    if data_key not in session:
        return jsonify({'error': 'Session expired'}), 400
    
    try:
        update_data = request.get_json()
        scraped_info = session[data_key]
        data = scraped_info['data']
        
        # Update the data with new PassBook and Variance values
        for row_index, updates in update_data.items():
            row_idx = int(row_index)
            if row_idx < len(data):
                for key, value in updates.items():
                    if key in ['Principal_PassBook', 'Principal_Variance', 'CBU_PassBook', 'CBU_Variance', 'CBU_withdraw_PassBook', 'CBU_withdraw_Variance']:
                        data[row_idx][key] = str(value)
        
        # Update session
        session[data_key] = scraped_info
        return jsonify({'success': True})
        
    except Exception as e:
        app.logger.error(f"Error updating data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/download/<session_id>')
def download_excel(session_id):
    """Generate and download Excel file."""
    data_key = f'scraped_data_{session_id}'
    
    if data_key not in session:
        flash('Session expired or invalid. Please scrape again.', 'error')
        return redirect(url_for('index'))
    
    try:
        scraped_info = session[data_key]
        data = scraped_info['data']
        url = scraped_info['url']
        
        # Create DataFrame with proper column ordering
        column_order = ['Receipt No', 'Date', 'Principal', 'Pen', 'Principal_PassBook', 'Principal_Variance', 
                       'CBU', 'CBU_PassBook', 'CBU_Variance', 'CBU withdraw', 'CBU_withdraw_PassBook', 'CBU_withdraw_Variance', 'Collector']
        
        # Ensure all columns exist in the data
        for row in data:
            for col in column_order:
                if col not in row:
                    row[col] = ''
        
        # Create DataFrame and reorder columns
        df = pd.DataFrame(data)
        
        # Reorder columns to match our preferred order
        available_columns = [col for col in column_order if col in df.columns]
        if available_columns:
            df = df[available_columns]
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        
        # Write to Excel with formatting
        with pd.ExcelWriter(temp_file.name, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Payment Data', index=False)
            
            # Get workbook and worksheet for formatting
            workbook = writer.book
            worksheet = writer.sheets['Payment Data']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
        
        # Generate filename
        domain = urlparse(url).netloc.replace('www.', '')
        filename = f"payment_data_{domain}_{session_id[:8]}.xlsx"
        
        app.logger.info(f"Generated Excel file for session {session_id}")
        
        # Clean up session data after download
        session.pop(data_key, None)
        
        return send_file(temp_file.name, 
                        as_attachment=True, 
                        download_name=filename,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
    except Exception as e:
        app.logger.error(f"Error generating Excel file: {str(e)}")
        flash(f'Error generating Excel file: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'service': 'payment-data-scraper'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
