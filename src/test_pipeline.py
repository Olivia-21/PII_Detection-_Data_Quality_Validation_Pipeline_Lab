import unittest
import pandas as pd
import numpy as np
import os
import sys

# Import functions to test
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from cleaner import clean_phone, clean_date
from masker import mask_name, mask_email, mask_phone, mask_address, mask_dob
from validator import run_validation

class TestCleaningFunctions(unittest.TestCase):
    def test_clean_phone(self):
        self.assertEqual(clean_phone('555-123-4567'), '555-123-4567')
        self.assertEqual(clean_phone('(555) 234-5678'), '555-234-5678')
        self.assertEqual(clean_phone('555.789.0123'), '555-789-0123')
        self.assertEqual(clean_phone('5557890123'), '555-789-0123')
        # Test short/invalid phone returning original
        self.assertEqual(clean_phone('123'), '123')
        # Test NaN
        self.assertTrue(pd.isna(clean_phone(np.nan)))

    def test_clean_date(self):
        self.assertEqual(clean_date('1985-03-15'), '1985-03-15')
        self.assertEqual(clean_date('1975/05/10'), '1975-05-10')
        self.assertEqual(clean_date('01/15/2024'), '2024-01-15')
        self.assertEqual(clean_date('invalid_date'), '1999-01-01')
        self.assertEqual(clean_date(np.nan), '1999-01-01')

class TestMaskingFunctions(unittest.TestCase):
    def test_mask_name(self):
        self.assertEqual(mask_name('John'), 'J***')
        self.assertEqual(mask_name('Jane'), 'J***')
        self.assertTrue(pd.isna(mask_name(np.nan)))

    def test_mask_email(self):
        self.assertEqual(mask_email('john.doe@gmail.com'), 'j***@gmail.com')
        self.assertEqual(mask_email('jane.smith@company.com'), 'j***@company.com')
        self.assertEqual(mask_email('invalidemail'), 'invalidemail')
        self.assertTrue(pd.isna(mask_email(np.nan)))

    def test_mask_phone(self):
        self.assertEqual(mask_phone('555-123-4567'), '***-***-4567')
        self.assertEqual(mask_phone('555-987-6543'), '***-***-6543')
        self.assertEqual(mask_phone('123'), '***-***-****')
        self.assertTrue(pd.isna(mask_phone(np.nan)))

    def test_mask_address(self):
        self.assertEqual(mask_address('123 Main St New York NY 10001'), '[MASKED ADDRESS]')
        self.assertTrue(pd.isna(mask_address(np.nan)))

    def test_mask_dob(self):
        self.assertEqual(mask_dob('1985-03-15'), '1985-**-**')
        self.assertEqual(mask_dob('1990-07-22'), '1990-**-**')
        self.assertEqual(mask_dob('123'), '123')
        self.assertTrue(pd.isna(mask_dob(np.nan)))

class TestPipelineOutput(unittest.TestCase):
    def test_cleaned_data_schema(self):
        """Test if the cleaned data structurally matches expectations and passes validation logic natively"""
        if not os.path.exists("output/customers_cleaned.csv"):
            self.skipTest("Pipeline not run, customers_cleaned.csv missing")
            
        df = pd.read_csv("output/customers_cleaned.csv")
        self.assertEqual(len(df.columns), 10)
        self.assertIn("customer_id", df.columns)
        self.assertIn("account_status", df.columns)
        
        # Check specific cleaning effects implicitly
        self.assertFalse(df['phone'].str.contains(r'\.').any())
        self.assertFalse(df['phone'].str.contains(r'\(').any())
        
        # No empty names
        self.assertEqual(df['first_name'].isna().sum(), 0)
        self.assertEqual(df['last_name'].isna().sum(), 0)

    def test_masked_data_schema(self):
        """Verify the masking completely obfuscates sensitive values"""
        if not os.path.exists("output/customers_masked.csv"):
            self.skipTest("Pipeline not run, customers_masked.csv missing")
            
        df = pd.read_csv("output/customers_masked.csv")
        
        # Names should contain ***
        self.assertTrue(df['first_name'].str.contains(r'\*\*\*').all())
        
        # Emails should have ***
        self.assertTrue(df['email'].str.contains(r'\*\*\*@').all())
        
        # Phones should start with ***-***-
        self.assertTrue(df['phone'].str.startswith('***-***-').all())
        
        # Addresses should be masked
        self.assertTrue((df['address'] == '[MASKED ADDRESS]').all())

if __name__ == '__main__':
    unittest.main()
