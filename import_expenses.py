from os.path import basename, splitext
import time
import subprocess
import records
from prompt_toolkit import prompt
from prompt_toolkit.contrib.completers import WordCompleter

chatty = False

def run(cmd):
    if chatty:
        print('INFO: Running...\n\t', cmd)
    subprocess.run(cmd, shell=True, check=True)

def create_tmp_db():
    dbname = 'expenses_workbench_%s' % time.strftime('%Y_%m_%d_%H_%M_%S')
    run('psql -c "create database %s"' % dbname)
    return dbname

def import_files(files, dbname):
    imported_tables = []
    for fname in files:
        confirm_import = input('Do you want to import %s? (y/N)' % fname)
        if confirm_import.lower() != 'y':
            continue
        table = splitext(basename(fname))[0]
        print('importing %s into %s ...' % (fname, table))
        run('csvsql --db postgres:///%s --no-constraints --insert %s' %
                (dbname, fname))
        run('psql -c "ALTER TABLE %s ADD COLUMN category VARCHAR" postgres:///%s' % (table, dbname))
        imported_tables.append(table)
    return imported_tables

def import_categories(categories_file, dbname):
    run('psql -c "drop table if exists categories;" postgres:///%s' %
            dbname)
    run('csvsql --db postgres:///%s --table categories --no-constraint --insert %s ' %
            (dbname, categories_file))

def apply_categories(tables, dbname):
    for table in tables:
        print ("Applying categories to %s" % table)
        run('''psql -c "update %s a \
                set category = c.category from categories c \
                where a.category is null and \
                a.\\\"Description\\\" ilike concat('%%', c.match_term, '%%');" \
                postgres:///%s''' % (table, dbname))

def uncategorized_stat(tables, dbname):
    db = records.Database('postgres:///%s' % dbname)
    rows = db.query('select * from categories')
    categories = {row.category for row in rows.all()}
    print ('Uncategorized stats:')
    for table in tables:
        print ('Table:', table)
        rows = db.query('''select count(*) FROM %s j \
                LEFT OUTER JOIN categories c ON j.\"Description\" \
                ILIKE CONCAT('%%', c.match_term, '%%') \
                WHERE c.category is NULL''' % table)
        print('Number of missing entries:', rows[0].count)
        rows = db.query('''select * FROM %s j \
                LEFT OUTER JOIN categories c ON j.\"Description\" \
                ILIKE CONCAT('%%', c.match_term, '%%') \
                WHERE c.category is NULL''' % table)
        print('Missing entries:', rows.dataset)
        ans = input('Would you like to categorize them? (Y/n)')
        if ans.lower() == 'y' or ans == '':
            for row in rows.all():
                print(row.dataset)
                completer = WordCompleter(row.Description.split(),
                        ignore_case=True)
                match = prompt('Match Term:', completer=completer)
                if not match:
                    continue
                completer = WordCompleter(categories, ignore_case=True)
                cat = prompt('Category:', completer=completer)
                if not cat:
                    continue
                db.query('INSERT INTO categories VALUES (:match, :cat)',
                        match=match, cat=cat)

def drop_db(dbname):
    run('psql -c "drop database %s"' % dbname)


def main(pattern):
    dbname = create_tmp_db()
    print('Successfully created:', dbname)
    cat_file = input('categories filename: [categories.csv]')
    if not cat_file:
        cat_file = 'categories.csv'
    import_categories(cat_file, dbname)
    print('Successfully imported categories.')
    tables = import_files(pattern, dbname)
    apply_categories(tables, dbname)
    uncategorized_stat(tables, dbname)
    cleanup = input('Cleanup the workbench? (y/N)')
    if cleanup.lower() == 'y':
        drop_db(dbname)

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
