# budget_tracker.py
"""
Budget Tracking Application

A Streamlit-based application for tracking business expenses and managing budgets.
Features:
- Bank statement import (PDF and Excel)
- Transaction management
- Budget tracking
- Payment request management
- Financial analytics
- Expense categorization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, time, timedelta
import os
from pathlib import Path
import numpy as np
from io import BytesIO
import sqlite3
from decimal import Decimal
import pdfplumber
import re
from time import sleep

class BankStatementParser:
    """Handles parsing of bank statements in various formats"""
    
    def __init__(self):
        self.supported_banks = ['HDFC']
        
    def clean_amount(self, amount_str):
        """Clean amount strings and convert to float"""
        try:
            if pd.isna(amount_str) or amount_str is None or amount_str == '':
                return 0.0
                
            if isinstance(amount_str, (int, float)):
                return float(amount_str)
            
            amount_str = str(amount_str).strip()
            amount_str = ''.join(c for c in amount_str if c.isdigit() or c == '.')
            
            return float(amount_str) if amount_str else 0.0
            
        except Exception as e:
            print(f"Error cleaning amount {amount_str}: {str(e)}")
            return 0.0

    def parse_date(self, date_val):
        """Parse date values from various formats"""
        try:
            if isinstance(date_val, datetime):
                return date_val
                
            if isinstance(date_val, str):
                date_str = date_val.strip()
                try:
                    return datetime.strptime(date_str, '%d/%m/%y')
                except ValueError:
                    try:
                        return datetime.strptime(date_str, '%d/%m/%Y')
                    except ValueError:
                        print(f"Could not parse date: {date_str}")
                        return None
            
            return None
            
        except Exception as e:
            print(f"Error parsing date {date_val}: {str(e)}")
            return None

    def parse_excel_statement(self, file_content):
        """Parse bank statement in Excel format"""
        try:
            print("\nProcessing Excel file...")
            df = pd.read_excel(BytesIO(file_content))
            
            # Find header row
            header_row = None
            for idx, row in df.iterrows():
                row_text = ' '.join(str(x).lower() for x in row.values)
                if ('date' in row_text and 'narration' in row_text and 
                    ('withdrawal' in row_text or 'debit' in row_text)):
                    header_row = idx
                    break
            
            if header_row is None:
                raise ValueError("Could not find transaction table header")
                
            df.columns = df.iloc[header_row]
            df = df.iloc[header_row + 1:].reset_index(drop=True)
            df.columns = df.columns.str.strip().str.lower()
            
            # Find required columns
            date_col = next(col for col in df.columns if 'date' in col.lower() and 'value' not in col.lower())
            narration_col = next(col for col in df.columns if 'narration' in col.lower())
            ref_col = next(col for col in df.columns if 'ref' in col.lower() or 'chq' in col.lower())
            withdrawal_col = next(col for col in df.columns if 'withdrawal' in col.lower() or 'debit' in col.lower())
            deposit_col = next(col for col in df.columns if 'deposit' in col.lower() or 'credit' in col.lower())
            balance_col = next(col for col in df.columns if 'balance' in col.lower())
            
            transactions = []
            current_description = ''
            
            for idx, row in df.iterrows():
                try:
                    if 'statement summary' in str(row[narration_col]).lower():
                        break
                        
                    date_val = row[date_col]
                    if pd.isna(date_val):
                        if current_description:
                            current_description += ' ' + str(row[narration_col])
                        continue
                    
                    date_obj = self.parse_date(date_val)
                    if not date_obj:
                        continue
                    
                    description = (current_description + ' ' + str(row[narration_col]) 
                                 if current_description else str(row[narration_col]))
                    current_description = ''
                    
                    withdrawal = self.clean_amount(row[withdrawal_col])
                    deposit = self.clean_amount(row[deposit_col])
                    balance = self.clean_amount(row[balance_col])
                    
                    transaction = {
                        'date': date_obj,
                        'description': description.strip(),
                        'reference': str(row[ref_col]).strip(),
                        'type': 'debit' if withdrawal > 0 else 'credit',
                        'amount': withdrawal if withdrawal > 0 else deposit,
                        'balance': balance
                    }
                    
                    transactions.append(transaction)
                    
                except Exception as e:
                    print(f"Error processing row {idx}: {str(e)}")
                    continue
            
            if not transactions:
                raise ValueError("No valid transactions found in the statement")
            
            return pd.DataFrame(transactions)
            
        except Exception as e:
            raise ValueError(f"Error processing Excel statement: {str(e)}")

    def parse_pdf_statement(self, file):
        """Parse bank statement in PDF format"""
        try:
            transactions = []
            
            with pdfplumber.open(file) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    
                    for table in tables:
                        if not table:
                            continue
                            
                        header_found = False
                        for row_idx, row in enumerate(table):
                            row = [str(cell).strip() if cell is not None else '' for cell in row]
                            row_text = ' '.join(row).lower()
                            
                            if ('date' in row_text and 'narration' in row_text and 
                                ('withdrawal' in row_text or 'debit' in row_text)):
                                header_found = True
                                continue
                            
                            if not header_found:
                                continue
                                
                            try:
                                if not row[0] or 'statement summary' in ' '.join(row).lower():
                                    continue
                                
                                date_obj = self.parse_date(row[0])
                                if not date_obj:
                                    continue
                                
                                withdrawal = self.clean_amount(row[4])
                                deposit = self.clean_amount(row[5])
                                
                                transaction = {
                                    'date': date_obj,
                                    'description': row[1].strip(),
                                    'reference': row[2].strip(),
                                    'type': 'debit' if withdrawal > 0 else 'credit',
                                    'amount': withdrawal if withdrawal > 0 else deposit,
                                    'balance': self.clean_amount(row[6])
                                }
                                
                                transactions.append(transaction)
                                
                            except Exception as e:
                                print(f"Error processing row: {str(e)}")
                                continue
            
            if not transactions:
                raise ValueError("No valid transactions found in the statement")
                
            return pd.DataFrame(transactions)
            
        except Exception as e:
            raise ValueError(f"Error processing PDF statement: {str(e)}")

class BudgetTracker:
    """Main class for budget and expense tracking functionality"""
    
    def __init__(self):
        """Initialize the budget tracker with database connection"""
        self.conn = sqlite3.connect('budget_tracker.db', check_same_thread=False)
        self.statement_parser = BankStatementParser()
        self.setup_database()
        
    def setup_database(self):
        """Set up the necessary database tables"""
        cursor = self.conn.cursor()
        
        # Create transactions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                description TEXT NOT NULL,
                reference_no TEXT,
                type TEXT CHECK(type IN ('debit', 'credit')) NOT NULL,
                amount DECIMAL(10,2) NOT NULL DEFAULT 0,
                balance DECIMAL(10,2) NOT NULL DEFAULT 0,
                category TEXT,
                tags TEXT,
                source TEXT NOT NULL DEFAULT 'manual',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create budgets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS budgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create categories table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                type TEXT CHECK(type IN ('expense', 'income', 'transfer')) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create payment request tickets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payment_request_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_number TEXT UNIQUE NOT NULL,
                department TEXT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                description TEXT NOT NULL,
                purpose TEXT NOT NULL,
                vendor_name TEXT,
                vendor_account TEXT,
                supporting_documents TEXT,
                status TEXT CHECK(status IN ('pending', 'approved', 'rejected', 'dispatched')) NOT NULL DEFAULT 'pending',
                priority TEXT CHECK(priority IN ('low', 'medium', 'high', 'urgent')) NOT NULL DEFAULT 'medium',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Add default categories
        default_categories = [
            ('Salary', 'income', 'Regular salary income'),
            ('Sales Revenue', 'income', 'Income from sales'),
            ('Office Supplies', 'expense', 'Office supplies and stationery'),
            ('Utilities', 'expense', 'Electricity, water, internet, etc.'),
            ('Marketing', 'expense', 'Marketing and advertising expenses'),
            ('Travel', 'expense', 'Business travel expenses'),
            ('Transfer', 'transfer', 'Internal transfers'),
            ('Other', 'expense', 'Miscellaneous expenses')
        ]
        
        cursor.executemany('''
            INSERT OR IGNORE INTO categories (name, type, description)
            VALUES (?, ?, ?)
        ''', default_categories)
        
        self.conn.commit()

    def process_bank_statement(self, file, file_type):
        """Process uploaded bank statement"""
        try:
            file_content = file.read()
            file.seek(0)
            
            if file_type.lower() == 'pdf':
                df = self.statement_parser.parse_pdf_statement(file)
            else:
                df = self.statement_parser.parse_excel_statement(file_content)
            
            cursor = self.conn.cursor()
            transactions_added = 0
            duplicates_found = 0
            
            for _, row in df.iterrows():
                cursor.execute('''
                    SELECT COUNT(*) FROM transactions
                    WHERE date = ? AND description = ? AND amount = ? AND type = ?
                ''', (
                    row['date'].strftime('%Y-%m-%d'),
                    row['description'],
                    float(row['amount']),
                    row['type']
                ))
                
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                        INSERT INTO transactions 
                        (date, description, reference_no, type, amount, balance, source)
                        VALUES (?, ?, ?, ?, ?, ?, 'bank_statement')
                    ''', (
                        row['date'].strftime('%Y-%m-%d'),
                        row['description'],
                        row['reference'],
                        row['type'],
                        float(row['amount']),
                        float(row['balance'])
                    ))
                    transactions_added += 1
                else:
                    duplicates_found += 1
            
            self.conn.commit()
            
            message = f"Successfully processed {transactions_added} new transaction(s)"
            if duplicates_found > 0:
                message += f" and skipped {duplicates_found} duplicate(s)"
            
            return True, message
            
        except Exception as e:
            return False, str(e)

    def add_manual_transaction(self, date, description, amount, category, transaction_type, reference_no=None):
        """Add a manual transaction"""
        cursor = self.conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO transactions 
                (date, description, reference_no, type, amount, category, source)
                VALUES (?, ?, ?, ?, ?, ?, 'manual')
            ''', (date, description, reference_no, transaction_type, amount, category))
            
            transaction_id = cursor.lastrowid
            self.conn.commit()
            return transaction_id
            
        except Exception as e:
            self.conn.rollback()
            raise e

    def delete_manual_transaction(self, transaction_id):
        """Delete a manually entered transaction"""
        cursor = self.conn.cursor()
        
        try:
            cursor.execute('SELECT source FROM transactions WHERE id = ?', (transaction_id,))
            result = cursor.fetchone()
            
            if not result or result[0] != 'manual':
                raise ValueError("Only manually entered transactions can be deleted")
            
            cursor.execute('DELETE FROM transactions WHERE id = ?', (transaction_id,))
            self.conn.commit()
            return True, "Transaction deleted successfully"
            
        except Exception as e:
            self.conn.rollback()
            return False, f"Error deleting transaction: {str(e)}"

    def get_transactions(self, start_date=None, end_date=None, category=None, transaction_type=None):
        """Retrieve transactions with filters"""
        query = "SELECT * FROM transactions WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        if category:
            query += " AND category = ?"
            params.append(category)
        if transaction_type:
            query += " AND type = ?"
            params.append(transaction_type)
            
        query += " ORDER BY date DESC, id DESC"
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    def get_transaction_summary(self, start_date=None, end_date=None):
        """Get transaction summary including total debits, credits, and balance"""
        cursor = self.conn.cursor()
        
        query = '''
            SELECT 
                COUNT(*) as total_transactions,
                COALESCE(SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END), 0) as total_debits,
                COALESCE(SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END), 0) as total_credits,
                COALESCE((SELECT balance FROM transactions 
                 WHERE date <= ? ORDER BY date DESC, id DESC LIMIT 1), 0) as closing_balance
            FROM transactions
            WHERE date BETWEEN ? AND ?
        '''
        
        end_date = end_date or datetime.now().date()
        start_date = start_date or (end_date - timedelta(days=30))
        
        cursor.execute(query, (end_date, start_date, end_date))
        result = cursor.fetchone()
        
        return (
            result[0] or 0,
            float(result[1] or 0),
            float(result[2] or 0),
            float(result[3] or 0)
        )

    def get_category_summary(self, start_date=None, end_date=None, transaction_type=None):
        """Get summary of transactions by category"""
        query = '''
            SELECT 
                COALESCE(category, 'Uncategorized') as category,
                SUM(amount) as total_amount,
                COUNT(*) as transaction_count
            FROM transactions
            WHERE date BETWEEN ? AND ?
        '''
        params = [start_date or '1900-01-01', end_date or '9999-12-31']
        
        if transaction_type:
            query += " AND type = ?"
            params.append(transaction_type)
            
        query += " GROUP BY category ORDER BY total_amount DESC"
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    def generate_ticket_number(self):
        """Generate a unique ticket number for payment requests"""
        cursor = self.conn.cursor()
        today = datetime.now().strftime('%Y%m%d')
        
        cursor.execute("""
            SELECT ticket_number FROM payment_request_tickets
            WHERE ticket_number LIKE ?
            ORDER BY ticket_number DESC LIMIT 1
        """, (f'PR-{today}-%',))
        
        result = cursor.fetchone()
        if result:
            last_number = int(result[0].split('-')[-1])
            new_number = str(last_number + 1).zfill(4)
        else:
            new_number = '0001'
            
        return f'PR-{today}-{new_number}'

    def create_payment_request(self, department, amount, description, purpose,
                             vendor_name=None, vendor_account=None, priority='medium'):
        """Create a new payment request ticket"""
        cursor = self.conn.cursor()
        
        try:
            ticket_number = self.generate_ticket_number()
            
            cursor.execute("""
                INSERT INTO payment_request_tickets (
                    ticket_number, department, amount, description,
                    purpose, vendor_name, vendor_account, priority
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ticket_number, department, amount, description,
                purpose, vendor_name, vendor_account, priority
            ))
            
            ticket_id = cursor.lastrowid
            self.conn.commit()
            return ticket_id, ticket_number
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error creating payment request: {str(e)}")

    def update_payment_request(self, ticket_id, status, comments=None):
        """Update payment request status"""
        cursor = self.conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE payment_request_tickets
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (status, ticket_id))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error updating payment request: {str(e)}")

    def get_payment_requests(self, status=None, department=None):
        """Get payment requests with optional filters"""
        query = "SELECT * FROM payment_request_tickets WHERE 1=1"
        params = []
        
        if status:
            query += " AND status = ?"
            params.append(status)
        if department:
            query += " AND department = ?"
            params.append(department)
            
        query += " ORDER BY created_at DESC"
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

class BudgetTrackerUI:
    """User interface class for the budget tracking application"""
    
    def __init__(self):
        """Initialize the UI with BudgetTracker"""
        self.tracker = BudgetTracker()
        st.set_page_config(page_title="Budget Tracker", layout="wide")
    
    def run(self):
        """Main application entry point"""
        st.title("Budget and Expense Tracking Platform")
        
        # Main navigation
        page = st.sidebar.selectbox(
            "Navigation",
            ["Dashboard", "Transactions", "Bank Statements", 
             "Budgets", "Payment Requests", "Analytics"]
        )
        
        if page == "Dashboard":
            self.show_dashboard()
        elif page == "Transactions":
            self.show_transactions()
        elif page == "Bank Statements":
            self.show_bank_statements()
        elif page == "Budgets":
            self.show_budget_management()
        elif page == "Payment Requests":
            self.show_payment_requests()
        elif page == "Analytics":
            self.show_analytics()

    def show_dashboard(self):
        """Display the main dashboard with key metrics"""
        st.header("Dashboard")
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "From Date",
                value=datetime.now().date() - timedelta(days=30)
            )
        with col2:
            end_date = st.date_input("To Date", value=datetime.now().date())
        
        try:
            summary = self.tracker.get_transaction_summary(start_date, end_date)
            total_transactions, total_debits, total_credits, closing_balance = summary
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Transactions", f"{int(total_transactions):,}")
            with col2:
                st.metric("Total Debits", f"₹{float(total_debits):,.2f}")
            with col3:
                st.metric("Total Credits", f"₹{float(total_credits):,.2f}")
            with col4:
                st.metric("Closing Balance", f"₹{float(closing_balance):,.2f}")

            st.subheader("Recent Transactions")
            transactions = self.tracker.get_transactions(start_date, end_date)
            
            if transactions:
                df = pd.DataFrame(
                    transactions,
                    columns=['id', 'date', 'description', 'reference_no', 
                            'type', 'amount', 'balance', 'category', 
                            'tags', 'source', 'created_at']
                )
                
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df['amount'] = df.apply(lambda x: f"₹{x['amount']:,.2f}", axis=1)
                df['balance'] = df.apply(lambda x: f"₹{x['balance']:,.2f}", axis=1)
                
                def highlight_type(row):
                    if row['type'] == 'debit':
                        return ['background-color: #ffebee'] * len(row)
                    elif row['type'] == 'credit':
                        return ['background-color: #e8f5e9'] * len(row)
                    else:
                        return ['background-color: #f5f5f5'] * len(row)
                
                styled_df = (df.style
                            .apply(highlight_type, axis=1)
                            .set_properties(**{
                                'background-color': '#f5f5f5',
                                'color': 'black',
                                'border-color': '#ddd'
                            }))
                st.dataframe(styled_df, height=400)
            else:
                st.info("No transactions found for the selected period.")
                
        except Exception as e:
            st.error(f"Error loading dashboard: {str(e)}")

    def show_bank_statements(self):
        """Handle bank statement upload and processing"""
        st.header("Bank Statement Upload")
        
        st.info("""
        Upload your bank statement in PDF or Excel format. 
        The system will automatically process and categorize the transactions.
        """)
        
        file_type = st.radio(
            "Select statement format",
            options=['Excel', 'PDF'],
            format_func=lambda x: f"{x} format"
        )
        
        if file_type == 'PDF':
            uploaded_file = st.file_uploader(
                "Upload Bank Statement (PDF)", 
                type=['pdf']
            )
        else:
            uploaded_file = st.file_uploader(
                "Upload Bank Statement (Excel)", 
                type=['xls', 'xlsx']
            )
        
        if uploaded_file is not None:
            with st.spinner("Processing bank statement..."):
                try:
                    success, message = self.tracker.process_bank_statement(
                        uploaded_file, 
                        'pdf' if file_type == 'PDF' else 'xls'
                    )
                    
                    if success:
                        st.success(message)
                        
                        st.subheader("Processed Transactions")
                        transactions = self.tracker.get_transactions(
                            datetime.now().date() - timedelta(days=7)
                        )
                        
                        if transactions:
                            df = pd.DataFrame(
                                transactions,
                                columns=['id', 'date', 'description', 'reference_no', 
                                        'type', 'amount', 'balance', 'category', 
                                        'tags', 'source', 'created_at']
                            )
                            
                            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                            df['amount'] = df.apply(lambda x: f"₹{x['amount']:,.2f}", axis=1)
                            df['balance'] = df.apply(lambda x: f"₹{x['balance']:,.2f}", axis=1)
                            
                            st.dataframe(df)
                        else:
                            st.info("No recent transactions found.")
                    else:
                        st.error(message)
                except Exception as e:
                    st.error(f"Error processing statement: {str(e)}")

    def show_transactions(self):
        """Manual transaction entry and transaction history view"""
        st.header("Transaction Management")
        
        st.subheader("Add New Transaction")
        with st.form("transaction_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                date = st.date_input("Transaction Date")
                amount = st.number_input("Amount", min_value=0.0, step=0.01)
                transaction_type = st.selectbox(
                    "Transaction Type",
                    options=['debit', 'credit']
                )
                
            with col2:
                description = st.text_input("Description")
                category = st.selectbox(
                    "Category",
                    options=[
                        'Salary', 'Sales Revenue', 'Office Supplies',
                        'Utilities', 'Marketing', 'Travel', 'Other'
                    ]
                )
                reference_no = st.text_input("Reference Number (Optional)")
            
            submitted = st.form_submit_button("Add Transaction")
            if submitted:
                try:
                    self.tracker.add_manual_transaction(
                        date, description, amount, category,
                        transaction_type, reference_no
                    )
                    st.success("Transaction added successfully!")
                except Exception as e:
                    st.error(f"Error adding transaction: {str(e)}")
        
        st.subheader("Transaction History")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            filter_start_date = st.date_input(
                "From Date",
                value=datetime.now().date() - timedelta(days=30),
                key="filter_start_date"
            )
        with col2:
            filter_end_date = st.date_input(
                "To Date",
                value=datetime.now().date(),
                key="filter_end_date"
            )
        with col3:
            filter_type = st.selectbox(
                "Transaction Type",
                options=['All', 'debit', 'credit']
            )
        
        try:
            transactions = self.tracker.get_transactions(
                start_date=filter_start_date,
                end_date=filter_end_date,
                transaction_type=None if filter_type == 'All' else filter_type
            )
            
            if transactions:
                df = pd.DataFrame(
                    transactions,
                    columns=['id', 'date', 'description', 'reference_no', 
                            'type', 'amount', 'balance', 'category', 
                            'tags', 'source', 'created_at']
                )
                
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df['amount'] = df.apply(lambda x: f"₹{x['amount']:,.2f}", axis=1)
                df['balance'] = df.apply(lambda x: f"₹{x['balance']:,.2f}", axis=1)
                
                for index, row in df.iterrows():
                    with st.container():
                        col1, col2, col3, col4, col5 = st.columns([2, 3, 2, 2, 1])
                        
                        with col1:
                            st.write(f"**Date:** {row['date']}")
                        with col2:
                            st.write(f"**Description:** {row['description']}")
                        with col3:
                            st.write(f"**Amount:** {row['amount']}")
                        with col4:
                            st.write(f"**Type:** {row['type']}")
                        with col5:
                            if row['source'] == 'manual':
                                if st.button('Delete', key=f"del_{row['id']}"):
                                    success, message = self.tracker.delete_manual_transaction(row['id'])
                                    if success:
                                        st.success(message)
                                        sleep(1)
                                        st.rerun()
                                    else:
                                        st.error(message)
                        
                        st.write("---")
                
                def highlight_type(row):
                    if row['type'] == 'debit':
                        return ['background-color: #ffebee'] * len(row)
                    elif row['type'] == 'credit':
                        return ['background-color: #e8f5e9'] * len(row)
                    else:
                        return ['background-color: #f5f5f5'] * len(row)
                
                styled_df = (df.style
                            .apply(highlight_type, axis=1)
                            .set_properties(**{
                                'background-color': '#f5f5f5',
                                'color': 'black',
                                'border-color': '#ddd'
                            }))
                st.dataframe(styled_df, height=400)
            else:
                st.info("No transactions found for the selected period.")
        except Exception as e:
            st.error(f"Error loading transactions: {str(e)}")

    def show_budget_management(self):
        """Budget allocation and tracking interface"""
        st.header("Budget Management")
        
        st.subheader("Set Budget")
        with st.form("budget_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                category = st.selectbox(
                    "Category",
                    options=[
                        'Office Supplies', 'Utilities', 'Marketing',
                        'Travel', 'Other'
                    ]
                )
                amount = st.number_input("Monthly Budget Amount", min_value=0.0)
                
            with col2:
                start_date = st.date_input("Start Date")
                end_date = st.date_input("End Date")
                
            submitted = st.form_submit_button("Set Budget")
            if submitted:
                try:
                    cursor = self.tracker.conn.cursor()
                    cursor.execute('''
                        INSERT INTO budgets (category, amount, start_date, end_date)
                        VALUES (?, ?, ?, ?)
                    ''', (category, amount, start_date, end_date))
                    self.tracker.conn.commit()
                    st.success("Budget set successfully!")
                except Exception as e:
                    st.error(f"Error setting budget: {str(e)}")

        st.subheader("Current Budget Allocations")
        cursor = self.tracker.conn.cursor()
        cursor.execute('''
            SELECT category, amount, start_date, end_date
            FROM budgets
            WHERE end_date >= date('now')
            ORDER BY start_date DESC
        ''')
        budgets = cursor.fetchall()
        
        if budgets:
            for category, budget, start_date, end_date in budgets:
                with st.container():
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.write(f"**Category:** {category}")
                    with col2:
                        st.write(f"**Monthly Budget:** ₹{budget:,.2f}")
                    with col3:
                        st.write(f"**Period:** {start_date} to {end_date}")
                    
                    # Get actual spending
                    cursor.execute('''
                        SELECT COALESCE(SUM(amount), 0)
                        FROM transactions
                        WHERE category = ? 
                        AND type = 'debit'
                        AND date BETWEEN ? AND ?
                    ''', (category, start_date, end_date))
                    
                    actual_spending = cursor.fetchone()[0]
                    
                    # Progress bar for budget utilization
                    progress = min(100, (actual_spending / budget) * 100)
                    st.write(f"Budget Utilization: {progress:.1f}%")
                    st.progress(progress / 100)
                    
                    # Warning if over budget
                    if actual_spending > budget:
                        st.warning(f"Over budget by ₹{(actual_spending - budget):,.2f}")
                    
                    st.write("---")
        else:
            st.info("No active budgets found.")

    def show_payment_requests(self):
        """Payment request management interface"""
        st.header("Payment Request Management")
        
        tab1, tab2 = st.tabs(["Create Request", "View Requests"])
        
        with tab1:
            self.show_payment_request_form()
        
        with tab2:
            self.show_payment_request_list()
    
    def show_payment_request_form(self):
        """Display form for creating new payment requests"""
        st.subheader("Create Payment Request")
        
        with st.form("payment_request_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                department = st.selectbox(
                    "Department",
                    options=["Sales", "Marketing", "Operations", "Finance", "IT"]
                )
                amount = st.number_input("Amount", min_value=0.0, step=0.01)
                purpose = st.selectbox(
                    "Purpose",
                    options=["Vendor Payment", "Expense Reimbursement", 
                            "Advance Request", "Other"]
                )
                priority = st.select_slider(
                    "Priority",
                    options=["low", "medium", "high", "urgent"]
                )
                
            with col2:
                description = st.text_area("Description")
                vendor_name = st.text_input("Vendor Name (if applicable)")
                vendor_account = st.text_input("Vendor Account Details (if applicable)")
                
            submitted = st.form_submit_button("Submit Request")
            
            if submitted:
                try:
                    ticket_id, ticket_number = self.tracker.create_payment_request(
                        department=department,
                        amount=amount,
                        description=description,
                        purpose=purpose,
                        vendor_name=vendor_name,
                        vendor_account=vendor_account,
                        priority=priority
                    )
                    
                    st.success(f"""
                        Payment request created successfully!
                        Ticket Number: {ticket_number}
                    """)
                    
                except Exception as e:
                    st.error(f"Error creating payment request: {str(e)}")
    
    def show_payment_request_list(self):
        """Display list of payment requests"""
        st.subheader("Payment Requests")
        
        status_filter = st.selectbox(
            "Filter by Status",
            options=["All", "pending", "approved", "rejected", "dispatched"]
        )
        
        try:
            requests = self.tracker.get_payment_requests(
                None if status_filter == "All" else status_filter
            )
            
            if not requests:
                st.info("No payment requests found.")
                return
            
            for request in requests:
                with st.container():
                    col1, col2, col3, col4 = st.columns([2, 3, 2, 1])
                    
                    with col1:
                        st.write(f"**Ticket:** {request['ticket_number']}")
                        st.write(f"**Amount:** ₹{request['amount']:,.2f}")
                    
                    with col2:
                        st.write(f"**Description:** {request['description']}")
                        st.write(f"**Department:** {request['department']}")
                    
                    with col3:
                        st.write(f"**Status:** {request['status'].title()}")
                        st.write(f"**Priority:** {request['priority'].title()}")
                    
                    with col4:
                        if request['status'] == 'pending':
                            if st.button('Approve', key=f"approve_{request['id']}"):
                                self.tracker.update_payment_request(
                                    request['id'],
                                    'approved',
                                    "Approved"
                                )
                                st.rerun()
                            
                            if st.button('Reject', key=f"reject_{request['id']}"):
                                reason = st.text_input(
                                    "Rejection Reason",
                                    key=f"reason_{request['id']}"
                                )
                                if st.button('Confirm Reject', 
                                           key=f"confirm_reject_{request['id']}"):
                                    self.tracker.update_payment_request(
                                        request['id'],
                                        'rejected',
                                        reason
                                    )
                                    st.rerun()
                        
                        elif request['status'] == 'approved':
                            if st.button('Process Payment', 
                                       key=f"process_{request['id']}"):
                                self.process_payment(request)
                                st.rerun()
                    
                    with st.expander("View Details"):
                        st.write(f"**Purpose:** {request['purpose']}")
                        if request['vendor_name']:
                            st.write(f"**Vendor:** {request['vendor_name']}")
                        if request['vendor_account']:
                            st.write(f"**Vendor Account:** {request['vendor_account']}")
                    
                    st.write("---")
            
        except Exception as e:
            st.error(f"Error loading payment requests: {str(e)}")
    
    def process_payment(self, request):
        """Handle payment processing"""
        try:
            # Create transaction for the payment
            transaction_id = self.tracker.add_manual_transaction(
                date=datetime.now().date(),
                description=f"Payment for ticket {request['ticket_number']}: {request['description']}",
                amount=request['amount'],
                category='Payments',
                transaction_type='debit'
            )
            
            # Update request status
            self.tracker.update_payment_request(
                request['id'],
                'dispatched',
                f"Payment processed. Transaction ID: {transaction_id}"
            )
            
            st.success("Payment processed successfully!")
            
        except Exception as e:
            st.error(f"Error processing payment: {str(e)}")

    def show_analytics(self):
        """Financial analytics and visualizations interface"""
        st.header("Financial Analytics")
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "From Date",
                value=datetime.now().date() - timedelta(days=90),
                key="analytics_start_date"
            )
        with col2:
            end_date = st.date_input(
                "To Date",
                value=datetime.now().date(),
                key="analytics_end_date"
            )

        try:
            transactions = self.tracker.get_transactions(start_date, end_date)
            if not transactions:
                st.info("No transactions found for the selected period.")
                return

            df = pd.DataFrame(
                transactions,
                columns=['id', 'date', 'description', 'reference_no', 'type', 
                        'amount', 'balance', 'category', 'tags', 'source', 
                        'created_at']
            )
            df['date'] = pd.to_datetime(df['date'])

            # Monthly Overview
            st.subheader("Monthly Overview")
            monthly_data = df.groupby([df['date'].dt.strftime('%Y-%m'), 'type'])['amount'].sum().unstack()
            
            fig = go.Figure()
            if 'debit' in monthly_data.columns:
                fig.add_trace(
                    go.Bar(
                        x=monthly_data.index,
                        y=monthly_data['debit'],
                        name='Expenses',
                        marker_color='#ff6b6b'
                    )
                )
            if 'credit' in monthly_data.columns:
                fig.add_trace(
                    go.Bar(
                        x=monthly_data.index,
                        y=monthly_data['credit'],
                        name='Income',
                        marker_color='#51cf66'
                    )
                )
            
            fig.update_layout(
                title='Monthly Income vs Expenses',
                barmode='group',
                xaxis_title='Month',
                yaxis_title='Amount (₹)',
                hovermode='x unified'
            )
            st.plotly_chart(fig)

            # Category Analysis
            st.subheader("Expense Distribution by Category")
            category_data = df[df['type'] == 'debit'].groupby('category')['amount'].sum()
            
            if not category_data.empty:
                fig = px.pie(
                    values=category_data.values,
                    names=category_data.index,
                    title='Expense Distribution'
                )
                st.plotly_chart(fig)

            # Balance Trend
            st.subheader("Balance Trend")
            balance_data = df.sort_values('date')[['date', 'balance']].drop_duplicates('date')
            
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=balance_data['date'],
                    y=balance_data['balance'],
                    mode='lines+markers',
                    name='Balance',
                    line=dict(color='#339af0')
                )
            )
            
            fig.update_layout(
                title='Balance Trend Over Time',
                xaxis_title='Date',
                yaxis_title='Balance (₹)',
                hovermode='x unified'
            )
            st.plotly_chart(fig)

            # Key Statistics
            st.subheader("Key Statistics")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_daily_expense = df[df['type'] == 'debit']['amount'].mean()
                st.metric(
                    "Average Daily Expense",
                    f"₹{avg_daily_expense:,.2f}"
                )
            
            with col2:
                total_inflow = df[df['type'] == 'credit']['amount'].sum()
                total_outflow = df[df['type'] == 'debit']['amount'].sum()
                net_flow = total_inflow - total_outflow
                st.metric(
                    "Net Cash Flow",
                    f"₹{net_flow:,.2f}",
                    delta=f"₹{abs(net_flow):,.2f}",
                    delta_color="normal" if net_flow >= 0 else "inverse"
                )
            
            with col3:
                transaction_count = len(df)
                st.metric(
                    "Total Transactions",
                    f"{transaction_count:,}"
                )

        except Exception as e:
            st.error(f"Error generating analytics: {str(e)}")

# Main entry point
if __name__ == "__main__":
    app = BudgetTrackerUI()
    app.run()