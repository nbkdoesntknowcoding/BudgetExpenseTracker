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
    """
    Main user interface class for the budget tracking application.
    Handles all UI components and user interactions, integrating with
    the core BudgetTracker and BudgetManager functionality.
    """
    
    def __init__(self):
        """
        Initialize the UI with necessary components:
        - BudgetTracker for core functionality
        - BudgetManager for enhanced budget management
        - Streamlit page configuration
        """
        self.tracker = BudgetTracker()
        self.tracker.budget_manager = BudgetManager(self.tracker.conn)
        st.set_page_config(
            page_title="Budget Tracker",
            layout="wide",
            initial_sidebar_state="expanded"
        )

    def run(self):
        """
        Main application entry point. Sets up the navigation and
        routes to appropriate page handlers based on user selection.
        """
        st.title("Budget and Expense Tracking Platform")
        
        # Main navigation in sidebar
        page = st.sidebar.selectbox(
            "Navigation",
            ["Dashboard", "Transactions", "Bank Statements", 
             "Enhanced Budget Management", "Payment Requests", "Analytics"]
        )
        
        # Route to appropriate page handler
        if page == "Dashboard":
            self.show_dashboard()
        elif page == "Transactions":
            self.show_transactions()
        elif page == "Bank Statements":
            self.show_bank_statements()
        elif page == "Enhanced Budget Management":
            self.show_enhanced_budget_management()
        elif page == "Payment Requests":
            self.show_payment_requests()
        elif page == "Analytics":
            self.show_analytics()

    def show_dashboard(self):
        """
        Display the main dashboard with key metrics and recent activity.
        Includes summary statistics and visualizations of budget health.
        """
        st.header("Dashboard")
        
        # Date range selection for dashboard metrics
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "From Date",
                value=datetime.now().date() - timedelta(days=30)
            )
        with col2:
            end_date = st.date_input("To Date", value=datetime.now().date())
        
        try:
            # Get and display summary metrics
            summary = self.tracker.get_transaction_summary(start_date, end_date)
            total_transactions, total_debits, total_credits, closing_balance = summary
            
            # Display key metrics in columns
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Transactions", 
                    f"{int(total_transactions):,}",
                    help="Number of transactions in selected period"
                )
            with col2:
                st.metric(
                    "Total Debits",
                    f"₹{float(total_debits):,.2f}",
                    help="Total outgoing funds"
                )
            with col3:
                st.metric(
                    "Total Credits",
                    f"₹{float(total_credits):,.2f}",
                    help="Total incoming funds"
                )
            with col4:
                st.metric(
                    "Closing Balance",
                    f"₹{float(closing_balance):,.2f}",
                    delta=f"₹{float(total_credits - total_debits):,.2f}",
                    delta_color="normal" if total_credits >= total_debits else "inverse"
                )

            # Display recent transactions
            st.subheader("Recent Transactions")
            transactions = self.tracker.get_transactions(start_date, end_date)
            
            if transactions:
                df = pd.DataFrame(
                    transactions,
                    columns=['id', 'date', 'description', 'reference_no', 
                            'type', 'amount', 'balance', 'category', 
                            'tags', 'source', 'created_at']
                )
                
                # Format data for display
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df['amount'] = df.apply(lambda x: f"₹{x['amount']:,.2f}", axis=1)
                df['balance'] = df.apply(lambda x: f"₹{x['balance']:,.2f}", axis=1)
                
                # Apply conditional formatting
                def highlight_type(row):
                    if row['type'] == 'debit':
                        return ['background-color: #ffebee'] * len(row)
                    elif row['type'] == 'credit':
                        return ['background-color: #e8f5e9'] * len(row)
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

    def show_enhanced_budget_management(self):
        """
        Enhanced budget management interface with multiple tabs for
        different budget management functions.
        """
        st.header("Enhanced Budget Management")
        
        # Navigation tabs for different budget management functions
        tabs = st.tabs([
            "Budget Overview", 
            "Create Budget", 
            "Transaction Mapping",
            "Budget Analytics"
        ])
        
        # Handle each tab's content
        with tabs[0]:
            self.show_budget_overview()
        
        with tabs[1]:
            self.show_budget_creation()
        
        with tabs[2]:
            self.show_transaction_mapping()
        
        with tabs[3]:
            self.show_budget_analytics()
    def show_budget_overview(self):
        """
        Display comprehensive overview of all budgets and their utilization.
        This method provides a detailed view of budget performance including:
        - Period selection with budget counts
        - Summary metrics for the selected period
        - Individual budget progress with detailed metrics
        - Burn rate analysis and projections
        """
        st.subheader("Budget Overview")
        
        # First, retrieve all budget periods with their associated budget counts
        cursor = self.tracker.conn.cursor()
        cursor.execute("""
            SELECT 
                bp.id,
                bp.name,
                bp.start_date,
                bp.end_date,
                COUNT(b.id) as budget_count,
                bp.status
            FROM budget_periods bp
            LEFT JOIN enhanced_budgets b ON bp.id = b.period_id
            GROUP BY bp.id
            ORDER BY bp.start_date DESC
        """)
        periods = cursor.fetchall()
        
        if not periods:
            st.warning("No budget periods defined. Please create a budget period first.")
            st.write("""
            To get started:
            1. Go to the 'Create Budget' tab
            2. Choose 'Create New' under Budget Period
            3. Define your first budget period
            """)
            return
        
        # Create a user-friendly period selection dropdown
        period_options = [
            f"{p[1]} ({p[2]} to {p[3]}) - {p[4]} budgets - {p[5]}"
            for p in periods
        ]
        selected_index = st.selectbox(
            "Select Budget Period",
            range(len(periods)),
            format_func=lambda x: period_options[x]
        )
        selected_period = periods[selected_index]
        
        # Display period details for context
        st.write("### Period Details")
        st.write(f"**Period:** {selected_period[1]}")
        st.write(f"**Duration:** {selected_period[2]} to {selected_period[3]}")
        st.write(f"**Status:** {selected_period[5]}")
        st.write(f"**Number of Budgets:** {selected_period[4]}")
        
        # Retrieve all budgets for the selected period with detailed information
        cursor.execute("""
            SELECT 
                bc.name as category_name,
                b.amount as budget_amount,
                COALESCE(b.rollover_amount, 0) as rollover_amount,
                COALESCE(SUM(m.amount), 0) as spent_amount,
                COUNT(DISTINCT m.transaction_id) as transaction_count,
                b.alert_threshold,
                b.created_at,
                bc.id as category_id,
                b.id as budget_id
            FROM enhanced_budgets b
            JOIN budget_categories bc ON b.category_id = bc.id
            LEFT JOIN budget_transaction_mapping m ON b.id = m.budget_id
            WHERE b.period_id = ?
            GROUP BY b.id, bc.name
            ORDER BY bc.name
        """, (selected_period[0],))
        
        budgets = cursor.fetchall()
        
        if not budgets:
            st.info(f"""No budgets found for period: {selected_period[1]}
            
To create a budget:
1. Go to the 'Create Budget' tab
2. Select or create a category
3. Set the budget amount and period
4. Click 'Create Budget' to save""")
            return
        
        # Calculate and display summary metrics
        total_budget = sum(float(b[1]) + float(b[2]) for b in budgets)
        total_spent = sum(float(b[3]) for b in budgets)
        
        # Display summary metrics in columns
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Total Budget",
                f"₹{total_budget:,.2f}",
                help="Total budget allocated for this period"
            )
        
        with col2:
            utilization = (total_spent/total_budget * 100) if total_budget > 0 else 0
            st.metric(
                "Total Spent",
                f"₹{total_spent:,.2f}",
                delta=f"{utilization:.1f}%",
                delta_color="inverse"
            )
        
        with col3:
            st.metric(
                "Remaining Budget",
                f"₹{(total_budget - total_spent):,.2f}",
                help="Total remaining budget across all categories"
            )
        
        # Calculate period metrics for burn rate calculations
        start_date = datetime.strptime(selected_period[2], '%Y-%m-%d')
        end_date = datetime.strptime(selected_period[3], '%Y-%m-%d')
        total_days = (end_date - start_date).days
        days_elapsed = (datetime.now() - start_date).days
        days_remaining = (end_date - datetime.now()).days
        
        # Display individual budget progress
        st.subheader("Budget Progress by Category")
        
        for budget in budgets:
            category_name = budget[0]
            budget_amount = float(budget[1]) + float(budget[2])  # Include rollover
            spent_amount = float(budget[3])
            transaction_count = budget[4]
            alert_threshold = float(budget[5] or 80)  # Default to 80% if not set
            created_at = budget[6]
            
            with st.container():
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**{category_name}**")
                    progress = (spent_amount / budget_amount) if budget_amount > 0 else 0
                    st.progress(min(progress, 1.0))
                    
                    if progress >= (alert_threshold / 100):
                        st.warning(f"⚠️ Alert: Utilization {progress:.1%} exceeds threshold {alert_threshold:.1f}%")
                    elif progress > 1.0:
                        st.error(f"🚨 Over budget: {progress:.1%}")
                    
                with col2:
                    st.write(f"₹{spent_amount:,.2f} / ₹{budget_amount:,.2f}")
                
                with st.expander(f"View Details - {category_name}"):
                    detail_col1, detail_col2 = st.columns(2)
                    
                    # Calculate burn rate and projections
                    daily_burn = spent_amount / max(1, days_elapsed)
                    monthly_burn = daily_burn * 30
                    projected_total = spent_amount + (daily_burn * days_remaining)
                    
                    with detail_col1:
                        st.write(f"**Budget Details:**")
                        st.write(f"- Created: {created_at}")
                        st.write(f"- Allocated: ₹{budget_amount:,.2f}")
                        st.write(f"- Spent: ₹{spent_amount:,.2f}")
                        st.write(f"- Remaining: ₹{(budget_amount - spent_amount):,.2f}")
                        st.write(f"- Utilization: {(progress * 100):.1f}%")
                    
                    with detail_col2:
                        st.write(f"**Metrics:**")
                        st.write(f"- Daily Burn Rate: ₹{daily_burn:,.2f}")
                        st.write(f"- Monthly Burn Rate: ₹{monthly_burn:,.2f}")
                        st.write(f"- Transaction Count: {transaction_count}")
                        st.write(f"- Days Remaining: {max(0, days_remaining)}")
                    
                    # Display recent transactions for this budget
                    if transaction_count > 0:
                        st.write("**Recent Transactions:**")
                        cursor.execute("""
                            SELECT 
                                t.date,
                                t.description,
                                m.amount,
                                m.created_at
                            FROM budget_transaction_mapping m
                            JOIN transactions t ON m.transaction_id = t.id
                            WHERE m.budget_id = ?
                            ORDER BY t.date DESC
                            LIMIT 5
                        """, (budget[8],))
                        recent_transactions = cursor.fetchall()
                        
                        for txn in recent_transactions:
                            st.write(f"- {txn[0]}: ₹{float(txn[2]):,.2f} - {txn[1]}")
                    
                    if projected_total > budget_amount:
                        st.warning(f"""
                        ⚠️ Projected Overspend Warning:
                        At the current burn rate of ₹{daily_burn:,.2f}/day,
                        this budget is projected to exceed its limit by ₹{(projected_total - budget_amount):,.2f}
                        """)
                
                st.write("---")
    def show_budget_creation(self):
        """
        Interface for creating new budgets and categories.
        Includes forms for both category and budget creation.
        """
        st.subheader("Create New Budget")
        
        # Two columns: Categories and Budget Creation
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("### Budget Categories")
            
            # Form for creating new categories
            with st.form("category_form"):
                category_name = st.text_input("Category Name")
                category_desc = st.text_area("Description")
                
                # Get existing categories for parent selection
                cursor = self.tracker.conn.cursor()
                cursor.execute("SELECT id, name FROM budget_categories")
                categories = cursor.fetchall()
                
                parent_category = st.selectbox(
                    "Parent Category (Optional)",
                    options=[None] + categories,
                    format_func=lambda x: "None" if x is None else x[1]
                )
                
                if st.form_submit_button("Create Category"):
                    try:
                        parent_id = parent_category[0] if parent_category else None
                        self.tracker.budget_manager.create_budget_category(
                            category_name,
                            category_desc,
                            parent_id
                        )
                        st.success("Category created successfully!")
                        # Store success state in session state
                        st.session_state.category_created = True
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error creating category: {str(e)}")
        
        with col2:
            st.write("### Create Budget")
            
            with st.form("budget_form"):
                # Category selection
                cursor = self.tracker.conn.cursor()
                cursor.execute("SELECT id, name FROM budget_categories")
                categories = cursor.fetchall()
                categories_dict = dict(categories)
                
                if not categories:
                    st.warning("Please create a category first")
                    st.form_submit_button("Create Budget", disabled=True)
                    return
                
                selected_category = st.selectbox(
                    "Category",
                    options=categories,
                    format_func=lambda x: x[1]
                )
                
                # Period creation/selection
                period_type = st.radio(
                    "Budget Period",
                    options=["Create New", "Use Existing"]
                )
                
                if period_type == "Create New":
                    period_name = st.text_input("Period Name")
                    start_date = st.date_input("Start Date")
                    end_date = st.date_input("End Date")
                else:
                    cursor.execute("""
                        SELECT id, name, start_date, end_date 
                        FROM budget_periods 
                        WHERE end_date >= date('now')
                        ORDER BY start_date DESC
                    """)
                    periods = cursor.fetchall()
                    if not periods:
                        st.warning("No active periods found. Please create a new period.")
                        st.form_submit_button("Create Budget", disabled=True)
                        return
                        
                    selected_period = st.selectbox(
                        "Select Period",
                        options=periods,
                        format_func=lambda x: f"{x[1]} ({x[2]} to {x[3]})"
                    )
                
                # Budget details
                amount = st.number_input("Budget Amount", min_value=0.0, step=100.0)
                rollover = st.checkbox("Enable Rollover")
                alert_threshold = st.slider(
                    "Alert Threshold (%)", 
                    min_value=0, 
                    max_value=100, 
                    value=80
                )
                notes = st.text_area("Notes")
                
                # Submit form
                if st.form_submit_button("Create Budget"):
                    try:
                        # Validate inputs
                        if amount <= 0:
                            st.error("Budget amount must be greater than zero")
                            return
                            
                        # Create period if needed
                        if period_type == "Create New":
                            if start_date >= end_date:
                                st.error("End date must be after start date")
                                return
                                
                            # Create new period
                            period_id = self.tracker.budget_manager.create_budget_period(
                                period_name,
                                start_date,
                                end_date,
                                'active'
                            )
                            period_display_name = period_name
                        else:
                            period_id = selected_period[0]
                            period_display_name = selected_period[1]
                        
                        # Create the budget
                        budget_id = self.tracker.budget_manager.create_budget(
                            selected_category[0],
                            period_id,
                            amount,
                            rollover,
                            alert_threshold,
                            notes
                        )
                        
                        # Store success data in session state
                        st.session_state.budget_created = True
                        st.session_state.budget_details = {
                            'category': categories_dict[selected_category[0]],
                            'amount': amount,
                            'period': period_display_name
                        }
                        
                        # Show success message and details
                        st.success("Budget created successfully!")
                        st.write("**Budget Details:**")
                        st.write(f"- Category: {categories_dict[selected_category[0]]}")
                        st.write(f"- Amount: ₹{amount:,.2f}")
                        st.write(f"- Period: {period_display_name}")
                        
                        # Rerun to refresh the page
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error creating budget: {str(e)}")
                        st.error("Please try again or contact support if the problem persists.")

    def show_transaction_mapping(self):
        """
        Interface for mapping transactions to budgets.
        Displays unmapped transactions and allows assigning them to budgets.
        """
        st.subheader("Transaction Mapping")
        
        # Get unmapped debit transactions only
        cursor = self.tracker.conn.cursor()
        cursor.execute("""
            SELECT t.id, t.date, t.description, t.amount, t.type
            FROM transactions t
            LEFT JOIN budget_transaction_mapping m ON t.id = m.transaction_id
            WHERE m.id IS NULL 
            AND t.type = 'debit'  -- Only get debit transactions for budget mapping
            ORDER BY t.date DESC
            LIMIT 100
        """)
        unmapped_transactions = cursor.fetchall()
        
        if not unmapped_transactions:
            st.info("No unmapped transactions found.")
            return
        
        # Get active budgets
        cursor.execute("""
            SELECT b.id, bc.name, bp.name, b.amount
            FROM enhanced_budgets b
            JOIN budget_categories bc ON b.category_id = bc.id
            JOIN budget_periods bp ON b.period_id = bp.id
            WHERE bp.end_date >= date('now')
            ORDER BY bp.start_date DESC
        """)
        active_budgets = cursor.fetchall()
        
        if not active_budgets:
            st.warning("No active budgets found. Please create a budget first.")
            return
        
        st.write(f"Found {len(unmapped_transactions)} unmapped transactions")
        
        # Create mapping interface
        for transaction in unmapped_transactions:
            with st.container():
                col1, col2, col3 = st.columns([3, 2, 1])
                
                with col1:
                    st.write(f"**{transaction[2]}**")  # Description
                    st.write(f"Date: {transaction[1]}")
                
                with col2:
                    st.write(f"Amount: ₹{transaction[3]:,.2f}")
                    st.write(f"Type: {transaction[4]}")
                
                with col3:
                    selected_budget = st.selectbox(
                        "Map to Budget",
                        options=[None] + active_budgets,
                        format_func=lambda x: "Select..." if x is None else f"{x[1]} - {x[2]}",
                        key=f"budget_select_{transaction[0]}"
                    )
                    
                    if selected_budget:
                        if st.button("Map", key=f"map_btn_{transaction[0]}"):
                            try:
                                self.tracker.budget_manager.map_transaction_to_budget(
                                    transaction[0],
                                    selected_budget[0],
                                    transaction[3]
                                )
                                st.success("Transaction mapped successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error mapping transaction: {str(e)}")
                
                st.write("---")

    def show_budget_analytics(self):
        """
        Display detailed budget analytics and insights.
        Provides comprehensive visualizations and metrics for budget analysis.
        """
        st.subheader("Budget Analytics")
        
        # Time period selection
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "From Date",
                value=datetime.now().date() - timedelta(days=90)
            )
        with col2:
            end_date = st.date_input(
                "To Date",
                value=datetime.now().date()
            )
        
        # Get budget metrics
        metrics = self.tracker.budget_manager.calculate_budget_metrics()
        
        if not metrics:
            st.info("No budget data available for analysis.")
            return
        
        # Overall budget health
        st.write("### Overall Budget Health")
        
        total_budget = sum(m['total_budget'] for m in metrics)
        total_spent = sum(m['spent_amount'] for m in metrics)
        overall_health = (total_budget - total_spent) / total_budget if total_budget > 0 else 0
        
        health_color = (
            "red" if overall_health < 0 else
            "orange" if overall_health < 0.2 else
            "green"
        )
        
        st.write(f"Budget Health Score: ", 
                 f"<span style='color: {health_color}'>{overall_health:.1%}</span>", 
                 unsafe_allow_html=True)
        
        # Create visualization data
        budget_data = []
        for m in metrics:
            budget_data.append({
                'category': m['category'],
                'budget': m['total_budget'],
                'spent': m['spent_amount'],
                'remaining': m['total_budget'] - m['spent_amount'],
                'utilization': m['utilization_percentage']
            })
        
        df = pd.DataFrame(budget_data)
        
        # Budget utilization chart
        st.write("### Budget Utilization by Category")
        
        # Create stacked bar chart for budget utilization
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Spent',
            x=df['category'],
            y=df['spent'],
            marker_color='#ff6b6b'
        ))
        
        fig.add_trace(go.Bar(
            name='Remaining',
            x=df['category'],
            y=df['remaining'],
            marker_color='#51cf66'
        ))
        
        fig.update_layout(
            barmode='stack',
            title='Budget Utilization',
            xaxis_title='Category',
            yaxis_title='Amount (₹)',
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig)
        
        # Burn rate analysis
        st.write("### Burn Rate Analysis")
        
        burn_rates = pd.DataFrame([{
            'category': m['category'],
            'daily_burn_rate': m['burn_rate_daily'],
            'monthly_burn_rate': m['burn_rate_monthly'],
            'days_remaining': m['days_remaining']
        } for m in metrics])
        
        # Create bar chart for burn rates
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Monthly Burn Rate',
            x=burn_rates['category'],
            y=burn_rates['monthly_burn_rate'],
            marker_color='#339af0'
        ))
        
        fig.update_layout(
            title='Monthly Burn Rate by Category',
            xaxis_title='Category',
            yaxis_title='Burn Rate (₹/month)',
            showlegend=True
        )
        
        st.plotly_chart(fig)
        
        # Detailed metrics table
        st.write("### Detailed Metrics")
        
        # Create and format metrics dataframe
        metrics_df = pd.DataFrame(metrics)
        metrics_df = metrics_df[[
            'category', 'total_budget', 'spent_amount', 'remaining_amount',
            'utilization_percentage', 'burn_rate_monthly', 'days_remaining',
            'projected_variance', 'transaction_count'
        ]]
        
        metrics_df.columns = [
            'Category', 'Budget', 'Spent', 'Remaining', 'Utilization %',
            'Monthly Burn Rate', 'Days Remaining', 'Projected Variance',
            'Transaction Count'
        ]
        
        # Format numeric columns
        numeric_cols = ['Budget', 'Spent', 'Remaining', 'Monthly Burn Rate', 'Projected Variance']
        for col in numeric_cols:
            metrics_df[col] = metrics_df[col].apply(lambda x: f"₹{x:,.2f}")
        
        metrics_df['Utilization %'] = metrics_df['Utilization %'].apply(lambda x: f"{x:.1f}%")
        
        # Display the formatted table
        st.dataframe(metrics_df, height=400)
        
        # Download button for metrics
        csv = metrics_df.to_csv(index=False)
        st.download_button(
            label="Download Metrics CSV",
            data=csv,
            file_name="budget_metrics.csv",
            mime="text/csv"
        )

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
                        'pdf' if file_type == 'PDF' else 'excel'
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
        
        # Date range selection for analytics
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "From Date",
                value=datetime.now().date() - timedelta(days=90)
            )
        with col2:
            end_date = st.date_input(
                "To Date",
                value=datetime.now().date()
            )
        
        try:
            # Get transaction data
            transactions = self.tracker.get_transactions(start_date, end_date)
            if not transactions:
                st.info("No transactions found for the selected period.")
                return
            
            # Create DataFrame from transactions
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
            
            # Add bars for debits and credits
            if 'debit' in monthly_data.columns:
                fig.add_trace(go.Bar(
                    x=monthly_data.index,
                    y=monthly_data['debit'],
                    name='Expenses',
                    marker_color='#ff6b6b'
                ))
            
            if 'credit' in monthly_data.columns:
                fig.add_trace(go.Bar(
                    x=monthly_data.index,
                    y=monthly_data['credit'],
                    name='Income',
                    marker_color='#51cf66'
                ))
            
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
                # Create pie chart for expense distribution
                fig = px.pie(
                    values=category_data.values,
                    names=category_data.index,
                    title='Expense Distribution'
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig)

                # Create a bar chart for category-wise spending
                fig = go.Figure(data=[
                    go.Bar(
                        x=category_data.index,
                        y=category_data.values,
                        marker_color='#4dabf7'
                    )
                ])
                fig.update_layout(
                    title='Category-wise Spending',
                    xaxis_title='Category',
                    yaxis_title='Amount (₹)',
                    showlegend=False
                )
                st.plotly_chart(fig)

            # Balance Trend Analysis
            st.subheader("Balance Trend Analysis")
            balance_data = df.sort_values('date')[['date', 'balance']].drop_duplicates('date')
            
            # Create line chart for balance trend
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=balance_data['date'],
                    y=balance_data['balance'],
                    mode='lines+markers',
                    name='Balance',
                    line=dict(color='#339af0'),
                    hovertemplate='%{x}<br>Balance: ₹%{y:,.2f}<extra></extra>'
                )
            )
            
            fig.update_layout(
                title='Balance Trend Over Time',
                xaxis_title='Date',
                yaxis_title='Balance (₹)',
                hovermode='x unified'
            )
            st.plotly_chart(fig)

            # Transaction Pattern Analysis
            st.subheader("Transaction Pattern Analysis")
            
            # Daily transaction count
            daily_transactions = df.groupby([df['date'].dt.date, 'type']).size().unstack(fill_value=0)
            
            fig = go.Figure()
            if 'debit' in daily_transactions.columns:
                fig.add_trace(go.Scatter(
                    x=daily_transactions.index,
                    y=daily_transactions['debit'],
                    name='Debits',
                    mode='lines',
                    line=dict(color='#ff6b6b')
                ))
            if 'credit' in daily_transactions.columns:
                fig.add_trace(go.Scatter(
                    x=daily_transactions.index,
                    y=daily_transactions['credit'],
                    name='Credits',
                    mode='lines',
                    line=dict(color='#51cf66')
                ))
            
            fig.update_layout(
                title='Daily Transaction Volume',
                xaxis_title='Date',
                yaxis_title='Number of Transactions',
                hovermode='x unified'
            )
            st.plotly_chart(fig)

            # Key Statistics and Metrics
            st.subheader("Key Statistics")
            
            # Calculate statistics
            stats_col1, stats_col2, stats_col3 = st.columns(3)
            
            with stats_col1:
                avg_daily_expense = df[df['type'] == 'debit']['amount'].mean()
                st.metric(
                    "Average Daily Expense",
                    f"₹{avg_daily_expense:,.2f}",
                    help="Average amount spent per day"
                )
                
                transaction_count = len(df)
                st.metric(
                    "Total Transactions",
                    f"{transaction_count:,}",
                    help="Total number of transactions in the period"
                )
            
            with stats_col2:
                total_inflow = df[df['type'] == 'credit']['amount'].sum()
                total_outflow = df[df['type'] == 'debit']['amount'].sum()
                net_flow = total_inflow - total_outflow
                
                st.metric(
                    "Net Cash Flow",
                    f"₹{net_flow:,.2f}",
                    delta=f"₹{abs(net_flow):,.2f}",
                    delta_color="normal" if net_flow >= 0 else "inverse",
                    help="Total inflow minus total outflow"
                )
                
                avg_transaction = df['amount'].mean()
                st.metric(
                    "Average Transaction",
                    f"₹{avg_transaction:,.2f}",
                    help="Average transaction amount"
                )
            
            with stats_col3:
                largest_expense = df[df['type'] == 'debit']['amount'].max()
                st.metric(
                    "Largest Expense",
                    f"₹{largest_expense:,.2f}",
                    help="Largest single expense in the period"
                )
                
                unique_categories = df['category'].nunique()
                st.metric(
                    "Active Categories",
                    f"{unique_categories}",
                    help="Number of unique transaction categories"
                )

            # Detailed Transaction Analysis
            st.subheader("Detailed Transaction Analysis")
            
            # Transaction size distribution
            transaction_bins = pd.cut(
                df['amount'], 
                bins=[0, 1000, 5000, 10000, 50000, float('inf')],
                labels=['0-1k', '1k-5k', '5k-10k', '10k-50k', '50k+']
            )
            size_distribution = transaction_bins.value_counts().sort_index()
            
            fig = go.Figure(data=[
                go.Bar(
                    x=size_distribution.index,
                    y=size_distribution.values,
                    marker_color='#4dabf7'
                )
            ])
            fig.update_layout(
                title='Transaction Size Distribution',
                xaxis_title='Transaction Range (₹)',
                yaxis_title='Number of Transactions',
                showlegend=False
            )
            st.plotly_chart(fig)

            # Export functionality
            st.subheader("Export Analytics")
            
            # Prepare export data
            export_data = {
                'Transaction Summary': pd.DataFrame({
                    'Metric': ['Total Transactions', 'Total Inflow', 'Total Outflow', 'Net Flow', 'Average Transaction'],
                    'Value': [
                        transaction_count,
                        total_inflow,
                        total_outflow,
                        net_flow,
                        avg_transaction
                    ]
                }),
                'Category Analysis': pd.DataFrame({
                    'Category': category_data.index,
                    'Amount': category_data.values
                }),
                'Daily Transactions': daily_transactions.reset_index(),
                'Balance Trend': balance_data
            }
            
            # Create Excel download button
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                for sheet_name, data in export_data.items():
                    data.to_excel(writer, sheet_name=sheet_name, index=False)
            
            excel_data = output.getvalue()
            st.download_button(
                label="Download Full Analytics Report (Excel)",
                data=excel_data,
                file_name="financial_analytics_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        except Exception as e:
            st.error(f"Error generating analytics: {str(e)}")


class BudgetManager:
    """Enhanced budget management system with detailed tracking and analytics"""
    
    def __init__(self, conn):
        """Initialize budget manager with database connection"""
        self.conn = conn
        self.setup_database()
    
    def setup_database(self):
        """Set up enhanced budget tracking tables"""
        cursor = self.conn.cursor()
        
        # Create budget categories table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS budget_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                parent_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_id) REFERENCES budget_categories(id)
            )
        ''')
        
        # Create budget periods table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS budget_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                status TEXT CHECK(status IN ('active', 'closed', 'draft')) NOT NULL DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create enhanced budgets table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enhanced_budgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER NOT NULL,
                period_id INTEGER NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                rollover_enabled BOOLEAN DEFAULT FALSE,
                rollover_amount DECIMAL(10,2) DEFAULT 0,
                alert_threshold DECIMAL(5,2),  -- Percentage at which to alert
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES budget_categories(id),
                FOREIGN KEY (period_id) REFERENCES budget_periods(id)
            )
        ''')
        
        # Create budget transactions mapping table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS budget_transaction_mapping (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id INTEGER NOT NULL,
                budget_id INTEGER NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id),
                FOREIGN KEY (budget_id) REFERENCES enhanced_budgets(id)
            )
        ''')
        
        self.conn.commit()

    def create_budget_category(self, name, description=None, parent_id=None):
        """Create a new budget category"""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO budget_categories (name, description, parent_id)
                VALUES (?, ?, ?)
            ''', (name, description, parent_id))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error creating budget category: {str(e)}")

    def create_budget_period(self, name, start_date, end_date, status='draft'):
        """Create a new budget period"""
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO budget_periods (name, start_date, end_date, status)
                VALUES (?, ?, ?, ?)
            ''', (name, start_date, end_date, status))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error creating budget period: {str(e)}")

    def create_budget(self, category_id, period_id, amount, rollover_enabled=False, 
                 alert_threshold=None, notes=None):
        """
        Create a new budget for a category and period with comprehensive validation.
        
        Args:
            category_id: ID of the category to budget for
            period_id: ID of the budget period
            amount: Budget amount (must be positive)
            rollover_enabled: Whether to enable budget rollover to next period
            alert_threshold: Percentage at which to trigger alerts (default 80%)
            notes: Additional notes about the budget
            
        Returns:
            int: ID of the created budget
            
        Raises:
            ValueError: If inputs are invalid or constraints are violated
            Exception: For database errors or other issues
        """
        cursor = self.conn.cursor()
        try:
            # Input validation
            if amount <= 0:
                raise ValueError("Budget amount must be greater than zero")
            
            if alert_threshold is not None and (alert_threshold < 0 or alert_threshold > 100):
                raise ValueError("Alert threshold must be between 0 and 100")
            
            # Verify category exists and is valid
            cursor.execute('''
                SELECT COUNT(*), name, description
                FROM budget_categories 
                WHERE id = ?
            ''', (category_id,))
            cat_result = cursor.fetchone()
            if not cat_result or cat_result[0] == 0:
                raise ValueError(f"Category with ID {category_id} not found")
            
            # Verify period exists and is valid
            cursor.execute('''
                SELECT COUNT(*), name, start_date, end_date, status
                FROM budget_periods 
                WHERE id = ?
            ''', (period_id,))
            period_result = cursor.fetchone()
            if not period_result or period_result[0] == 0:
                raise ValueError(f"Period with ID {period_id} not found")
            
            # Verify period is active
            if period_result[4] != 'active':
                raise ValueError(f"Cannot create budget in {period_result[4]} period")
            
            # Check for existing budget in this category and period
            cursor.execute('''
                SELECT COUNT(*), amount, created_at 
                FROM enhanced_budgets 
                WHERE category_id = ? AND period_id = ?
            ''', (category_id, period_id))
            existing_budget = cursor.fetchone()
            if existing_budget and existing_budget[0] > 0:
                raise ValueError(
                    f"A budget of ₹{existing_budget[1]:,.2f} already exists for "
                    f"category '{cat_result[1]}' in period '{period_result[1]}' "
                    f"(created on {existing_budget[2]})"
                )
            
            # Create the budget with all necessary fields
            cursor.execute('''
                INSERT INTO enhanced_budgets (
                    category_id, 
                    period_id, 
                    amount, 
                    rollover_enabled, 
                    rollover_amount,
                    alert_threshold, 
                    notes, 
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                category_id,
                period_id,
                amount,
                rollover_enabled,
                0.0,  # Initial rollover amount is 0
                alert_threshold or 80.0,  # Default alert threshold
                notes
            ))
            
            budget_id = cursor.lastrowid
            
            # Verify the budget was created successfully
            cursor.execute('''
                SELECT COUNT(*), amount, created_at 
                FROM enhanced_budgets 
                WHERE id = ?
            ''', (budget_id,))
            verify_result = cursor.fetchone()
            if not verify_result or verify_result[0] == 0:
                raise ValueError("Budget creation failed - no record found after insert")
            
            # Commit the transaction
            self.conn.commit()
            
            return budget_id
            
        except Exception as e:
            # Rollback on any error
            self.conn.rollback()
            raise Exception(f"Error creating budget: {str(e)}")
            
        finally:
            # Ensure we always close the cursor
            cursor.close()

    def map_transaction_to_budget(self, transaction_id, budget_id, amount, notes=None):
        """
        Map a transaction to a specific budget.
        Only debit transactions can be mapped to budgets since credits represent revenue/investments.
        
        Args:
            transaction_id: ID of the transaction to map
            budget_id: ID of the budget to map to
            amount: Amount to allocate to this budget
            notes: Optional notes about this mapping
            
        Raises:
            ValueError: If transaction is not found or is a credit transaction
        """
        cursor = self.conn.cursor()
        try:
            # Verify transaction exists and get its details
            cursor.execute('SELECT type, amount FROM transactions WHERE id = ?', (transaction_id,))
            transaction = cursor.fetchone()
            if not transaction:
                raise ValueError("Transaction not found")
                
            # Enforce debit-only mapping
            if transaction[0] != 'debit':
                raise ValueError("Only debit transactions can be mapped to budgets. Credit transactions represent revenue/investments.")

            # Verify budget exists and get its details
            cursor.execute('''
                SELECT b.amount, b.rollover_enabled, b.rollover_amount, bp.start_date, bp.end_date
                FROM enhanced_budgets b
                JOIN budget_periods bp ON b.period_id = bp.id
                WHERE b.id = ?
            ''', (budget_id,))
            budget = cursor.fetchone()
            if not budget:
                raise ValueError("Budget not found")

            # Insert the mapping
            cursor.execute('''
                INSERT INTO budget_transaction_mapping 
                (transaction_id, budget_id, amount, notes)
                VALUES (?, ?, ?, ?)
            ''', (transaction_id, budget_id, amount, notes))
            
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error mapping transaction to budget: {str(e)}")

    def get_budget_utilization(self, budget_id):
        """Get detailed budget utilization metrics"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                b.amount as budget_amount,
                b.rollover_amount,
                COALESCE(SUM(m.amount), 0) as spent_amount,
                COUNT(m.id) as transaction_count,
                bp.start_date,
                bp.end_date,
                bc.name as category_name
            FROM enhanced_budgets b
            JOIN budget_periods bp ON b.period_id = bp.id
            JOIN budget_categories bc ON b.category_id = bc.id
            LEFT JOIN budget_transaction_mapping m ON b.id = m.budget_id
            WHERE b.id = ?
            GROUP BY b.id
        ''', (budget_id,))
        
        result = cursor.fetchone()
        if not result:
            raise ValueError("Budget not found")
            
        budget_amount = float(result[0]) + float(result[1])  # Include rollover
        spent_amount = float(result[2])
        
        return {
            'category': result[6],
            'budget_amount': budget_amount,
            'spent_amount': spent_amount,
            'remaining_amount': budget_amount - spent_amount,
            'utilization_percentage': (spent_amount / budget_amount * 100) if budget_amount > 0 else 0,
            'transaction_count': result[3],
            'start_date': result[4],
            'end_date': result[5],
            'daily_burn_rate': spent_amount / max(1, (datetime.now() - datetime.strptime(result[4], '%Y-%m-%d')).days),
            'projected_end_amount': budget_amount - (spent_amount * (datetime.strptime(result[5], '%Y-%m-%d') - datetime.strptime(result[4], '%Y-%m-%d')).days / 
                                                   max(1, (datetime.now() - datetime.strptime(result[4], '%Y-%m-%d')).days))
        }

    def get_budget_transactions(self, budget_id):
        """Get all transactions mapped to a specific budget"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT 
                t.date,
                t.description,
                t.type,
                m.amount as allocated_amount,
                t.amount as total_amount,
                m.notes
            FROM budget_transaction_mapping m
            JOIN transactions t ON m.transaction_id = t.id
            WHERE m.budget_id = ?
            ORDER BY t.date DESC
        ''', (budget_id,))
        
        return cursor.fetchall()

    def get_category_budgets(self, category_id, include_children=True):
        """Get all budgets for a category and optionally its subcategories"""
        cursor = self.conn.cursor()
        
        if include_children:
            # First get all child categories
            cursor.execute('''
                WITH RECURSIVE category_tree AS (
                    SELECT id, name, parent_id FROM budget_categories WHERE id = ?
                    UNION ALL
                    SELECT c.id, c.name, c.parent_id 
                    FROM budget_categories c
                    JOIN category_tree ct ON c.parent_id = ct.id
                )
                SELECT id FROM category_tree
            ''', (category_id,))
            category_ids = [row[0] for row in cursor.fetchall()]
        else:
            category_ids = [category_id]
            
        placeholders = ','.join('?' * len(category_ids))
        cursor.execute(f'''
            SELECT 
                bc.name as category_name,
                bp.name as period_name,
                b.amount,
                b.rollover_amount,
                COALESCE(SUM(m.amount), 0) as spent_amount,
                bp.start_date,
                bp.end_date,
                b.id as budget_id
            FROM enhanced_budgets b
            JOIN budget_categories bc ON b.category_id = bc.id
            JOIN budget_periods bp ON b.period_id = bp.id
            LEFT JOIN budget_transaction_mapping m ON b.id = m.budget_id
            WHERE b.category_id IN ({placeholders})
            GROUP BY b.id
            ORDER BY bp.start_date DESC
        ''', category_ids)
        
        return cursor.fetchall()

    def calculate_budget_metrics(self, period_id=None):
        """
        Calculate comprehensive budget metrics for analysis.
        
        Args:
            period_id: Optional period ID to filter by
            
        Returns:
            list: List of dictionaries containing metrics for each budget
        """
        cursor = self.conn.cursor()
        
        query = '''
            SELECT 
                bc.name as category_name,
                b.amount as budget_amount,
                b.rollover_amount,
                COALESCE(SUM(m.amount), 0) as spent_amount,
                bp.start_date,
                bp.end_date,
                COUNT(DISTINCT m.transaction_id) as transaction_count
            FROM enhanced_budgets b
            JOIN budget_categories bc ON b.category_id = bc.id
            JOIN budget_periods bp ON b.period_id = bp.id
            LEFT JOIN budget_transaction_mapping m ON b.id = m.budget_id
            WHERE 1=1
        '''
        
        params = []
        if period_id:
            query += " AND bp.id = ?"
            params.append(period_id)
            
        query += ' GROUP BY b.id, bc.name'
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        metrics = []
        for result in results:
            category_name = result[0]
            budget_amount = float(result[1])
            rollover_amount = float(result[2] or 0)
            spent_amount = float(result[3])
            start_date = datetime.strptime(result[4], '%Y-%m-%d')
            end_date = datetime.strptime(result[5], '%Y-%m-%d')
            transaction_count = result[6]
            
            total_budget = budget_amount + rollover_amount
            
            # Calculate days elapsed and remaining
            total_days = (end_date - start_date).days
            days_elapsed = max(1, (datetime.now() - start_date).days)
            days_remaining = max(0, (end_date - datetime.now()).days)
            
            # Calculate burn rate and projections
            burn_rate = spent_amount / max(1, days_elapsed)
            projected_total = spent_amount + (burn_rate * days_remaining)
            
            metrics.append({
                'category': category_name,
                'total_budget': total_budget,
                'spent_amount': spent_amount,
                'remaining_amount': total_budget - spent_amount,
                'utilization_percentage': (spent_amount / total_budget * 100) if total_budget > 0 else 0,
                'burn_rate_daily': burn_rate,
                'burn_rate_monthly': burn_rate * 30,
                'projected_total': projected_total,
                'projected_variance': total_budget - projected_total,
                'transaction_count': transaction_count,
                'average_transaction': spent_amount / transaction_count if transaction_count > 0 else 0,
                'days_remaining': days_remaining,
                'days_elapsed': days_elapsed,
                'total_days': total_days,
                'on_track': projected_total <= total_budget
            })
            
        return metrics
# Main entry point
if __name__ == "__main__":
    app = BudgetTrackerUI()
    app.run()
