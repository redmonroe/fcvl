class Persistent:
    damages = [
                {'morris, michael': (599, '2022-02-01', 'exterm')}, 
                {'greiner, richard': (146, '2022-10-01', 'ur sept_oct')}, 
                {'greiner, richard': (48, '2022-11-01', 'nov')}, 
    ]
    
    changes = [{'obj_type': 'Payment', 'action': 'delete', 'col_name1': ('tenant_id', 'newcomb, benny'), 'col_name2': ('amount', '476.0'), 'col_name3': ('date_posted', '2022-02-08')}, 
        
        {'obj_type': 'Payment', 'action': 'update_amount', 'col_name1': ('tenant_id', 'perry, roxanne'), 'col_name2':('amount', '123.00'), 'col_name3': ('date_posted', '2022-08-19'), 'col_name4': ('new_amount', '120.00')}, 

        {'obj_type': 'Payment', 'action': 'update_amount', 'col_name1': ('tenant_id', 'amsley, glen'), 'col_name2':('amount', '350.00'), 'col_name3': ('date_posted', '2022-08-04'), 'col_name4': ('new_amount', '300.50')}, 

        {'obj_type': 'Payment', 'action': 'delete', 'col_name1': ('tenant_id', 'newcomb, benny'), 'col_name2': ('amount', '400.00'), 'col_name3': ('date_posted', '2022-09-26')},

        ## should be able to erase Alicia Brown (following 2) as new sheet will have picked up IR
        {'obj_type': 'Subsidy', 'action': 'update_amount', 'col_name1': ('tenant_id', 'brown, alicia'), 'col_name2': ('sub_amount', '224.00'), 'col_name3': ('date_posted', '2022-07-01'), 'col_name4': ('new_amount', '521.00')},

        {'obj_type': 'TenantRent', 'action': 'update_amount', 'col_name1': ('t_name_id', 'brown, alicia'), 'col_name2': ('rent_amount', '546.00'), 'col_name3': ('rent_date', '2022-07-01'), 'col_name4': ('new_amount', '249.00')},

        {'obj_type': 'Subsidy', 'action': 'update_amount', 'col_name1': ('tenant_id', 'brown, alicia'), 'col_name2': ('sub_amount', '1115.00'), 'col_name3': ('date_posted', '2022-09-01'), 'col_name4': ('new_amount', '224.00')},

        {'obj_type': 'TenantRent', 'action': 'update_amount', 'col_name1': ('t_name_id', 'brown, alicia'), 'col_name2': ('rent_amount', '-345.00'), 'col_name3': ('rent_date', '2022-09-01'), 'col_name4': ('new_amount', '546.00')},

        {'obj_type': 'Subsidy', 'action': 'update_amount', 'col_name1': ('tenant_id', 'brown, alicia'), 'col_name2': ('sub_amount', '-370.00'), 'col_name3': ('date_posted', '2022-10-01'), 'col_name4': ('new_amount', '224.00')},

        {'obj_type': 'TenantRent', 'action': 'update_amount', 'col_name1': ('t_name_id', 'brown, alicia'), 'col_name2': ('rent_amount', '1140.00'), 'col_name3': ('rent_date', '2022-10-01'), 'col_name4': ('new_amount', '546.00')},

        {'obj_type': 'TenantRent', 'action': 'update_amount', 'col_name1': ('t_name_id', 'greiner, richard'), 'col_name2': ('rent_amount', '0'), 'col_name3': ('rent_date', '2022-09-01'), 'col_name4': ('new_amount', '-73.00')},

        {'obj_type': 'TenantRent', 'action': 'update_amount', 'col_name1': ('t_name_id', 'greiner, richard'), 'col_name2': ('rent_amount', '-74'), 'col_name3': ('rent_date', '2022-10-01'), 'col_name4': ('new_amount', '-73.00')},

        # {'obj_type': 'Subsidy', 'action': 'update_amount', 'col_name1': ('tenant_id', 'greiner, richard'), 'col_name2': ('sub_amount', '-3'), 'col_name3': ('date_posted', '2022-10-01'), 'col_name4': ('new_amount', '224.00')},
        
        {'obj_type': 'Payment', 'action': 'update_amount', 'col_name1': ('tenant_id', 'hawkins, norland'), 'col_name2': ('amount', '314.00'), 'col_name3': ('date_posted', '2022-11-11'), 'col_name4': ('new_amount', '204.00')},
    ]

    units = ['CD-A','CD-B','CD-101', 'CD-102', 'CD-104', 'CD-105', 'CD-106', 'CD-107', 'CD-108',
            'CD-109', 'CD-110', 'CD-111', 'CD-112','CD-114', 'CD-115', 'CD-201', 'CD-202',
            'CD-203',
            'CD-204',
            'CD-205',
            'CD-206',
            'CD-207',
            'CD-208',
            'CD-209',
            'CD-210',
            'CD-211',
            'CD-212',
            'CD-214',
            'CD-215',
            'CD-301',
            'CD-302',
            'CD-303',
            'CD-304',
            'CD-305',
            'CD-306',
            'CD-307',
            'CD-308',
            'CD-309',
            'CD-310',
            'CD-311',
            'CD-312',
            'CD-314',
            'CD-315',
            'PT-101',
            'PT-102',
            'PT-103',
            'PT-104',
            'PT-105',
            'PT-106',
            'PT-107',
            'PT-108',
            'PT-109',
            'PT-110',
            'PT-111',
            'PT-112',
            'PT-201',
            'PT-202',
            'PT-203',
            'PT-204',
            'PT-205',
            'PT-206',
            'PT-207',
            'PT-208',
            'PT-209',
            'PT-210',
            'PT-211',
            'PT-212',]
