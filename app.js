const mysql = require('mysql2/promise');
const { Client } = require('pg');
const fs = require('fs');

const mysqlConfig = { 
    host: 'localhost', 
    user: 'root', 
    password: '1', 
    database: 'store' 
};

const pgConfig = { 
    host: 'localhost', 
    user: 'postgres', 
    password: '1', 
    database: 'laravel8' 
};

function logToFile(message) {
    fs.appendFileSync('data_check_log.txt', `${new Date().toISOString()} - ${message}\n`);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function getForeignKeys(conn, dbType, database) {
    let query;
    if (dbType === 'mysql') {
        query = `
            SELECT 
                TABLE_NAME, 
                COLUMN_NAME, 
                REFERENCED_TABLE_NAME, 
                REFERENCED_COLUMN_NAME 
            FROM 
                information_schema.KEY_COLUMN_USAGE 
            WHERE 
                TABLE_SCHEMA = ? AND REFERENCED_TABLE_NAME IS NOT NULL;
        `;
        const [rows] = await conn.execute(query, [database]);
        return rows;
    } else if (dbType === 'postgres') {
        // Dinamik schema aniqlash
        const schemaQuery = `
            SELECT table_schema 
            FROM information_schema.tables 
            WHERE table_name = 'canceled_pre_order_product_infos' 
            LIMIT 1;
        `;
        const schemaResult = await conn.query(schemaQuery);
        const schema = schemaResult.rows.length > 0 ? schemaResult.rows[0].table_schema : 'public';

        query = `
            SELECT 
                tc.table_name AS TABLE_NAME, 
                kcu.column_name AS COLUMN_NAME, 
                ccu.table_name AS REFERENCED_TABLE_NAME, 
                ccu.column_name AS REFERENCED_COLUMN_NAME 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu 
                    ON tc.constraint_name = kcu.constraint_name 
                JOIN information_schema.constraint_column_usage AS ccu 
                    ON ccu.constraint_name = tc.constraint_name 
            WHERE 
                tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = $1;
        `;
        const result = await conn.query(query, [schema]);
        logToFile(`PostgreSQL schema: ${schema}, foreign keys found: ${result.rows.length}`);
        return result.rows;
    }
}

// Vaqtni moslashtirish uchun yordamchi funksiya
function normalizeDate(value) {
    if (value === null) return 'NULL';
    const date = new Date(value);
    return date.toISOString().replace('T', ' ').substring(0, 19); // "YYYY-MM-DD HH:MM:SS"
}

// Ustunlarni taqqoslash uchun yordamchi funksiya
function compareRows(mysqlRow, pgRow) {
    const mysqlKeys = Object.keys(mysqlRow).sort();
    const pgKeys = Object.keys(pgRow).sort();
    if (mysqlKeys.join() !== pgKeys.join()) {
        return { match: false, reason: `Ustun nomlari farq qiladi: MySQL=${mysqlKeys}, PostgreSQL=${pgKeys}` };
    }

    let differences = [];
    for (const key of mysqlKeys) {
        let mysqlValue = mysqlRow[key];
        let pgValue = pgRow[key];
        
        // Vaqt ustunlari uchun normalizatsiya
        if (key.includes('_at') || key === 'date') { 
            mysqlValue = normalizeDate(mysqlValue);
            pgValue = normalizeDate(pgValue);
        } else {
            mysqlValue = mysqlValue === null ? 'NULL' : String(mysqlValue);
            pgValue = pgValue === null ? 'NULL' : String(pgValue);
        }

        if (mysqlValue !== pgValue) {
            differences.push(`Ustun '${key}': MySQL="${mysqlValue}", PostgreSQL="${pgValue}"`);
        }
    }

    if (differences.length > 0) {
        return { match: false, reason: `Namuna ma'lumotlari farq qiladi: ${differences.join('; ')}` };
    }
    return { match: true, reason: '' };
}

async function checkSingleTable(table, mysqlConn, pgClient, foreignKeysMysql, foreignKeysPg) {
    let isMatch = true;
    let mismatchReason = '';

    try {
        logToFile(`${table}: Tekshiruv boshlandi`);

        // Qator sonini tekshirish
        const [mysqlCount] = await mysqlConn.execute(`SELECT COUNT(*) as count FROM ${table}`);
        const pgCount = await pgClient.query(`SELECT COUNT(*) as count FROM ${table}`);
        const mysqlRowCount = Number(mysqlCount[0].count);
        const pgRowCount = Number(pgCount.rows[0].count);

        logToFile(`${table}: MySQL qator soni - ${mysqlRowCount}, PostgreSQL qator soni - ${pgRowCount}`);
        if (mysqlRowCount !== pgRowCount) {
            isMatch = false;
            mismatchReason = `Qator soni farq qiladi: MySQL=${mysqlRowCount}, PostgreSQL=${pgRowCount}`;
            console.log(`${table}: ${mismatchReason}`);
            logToFile(`${table}: ${mismatchReason}`);
            return { isMatch, mismatchReason };
        }
        console.log(`${table}: Qator soni to'g'ri - ${mysqlRowCount}`);
        logToFile(`${table}: Qator soni mos keladi`);

        // ID larni taqqoslash
        const [mysqlIds] = await mysqlConn.execute(`SELECT id FROM ${table} ORDER BY id`);
        const pgIds = await pgClient.query(`SELECT id FROM ${table} ORDER BY id`);
        const mysqlIdList = mysqlIds.map(row => String(row.id));
        const pgIdList = pgIds.rows.map(row => String(row.id));

        logToFile(`${table}: ID lar tekshirildi - MySQL: ${mysqlIdList.length}, PostgreSQL: ${pgIdList.length}`);
        if (mysqlIdList.join() !== pgIdList.join()) {
            isMatch = false;
            mismatchReason = 'ID lar farq qiladi';
            console.log(`${table}: ${mismatchReason}`);
            logToFile(`${table}: ${mismatchReason}`);
            return { isMatch, mismatchReason };
        }
        console.log(`${table}: ID lar to'g'ri`);
        logToFile(`${table}: ID lar mos keladi`);

        // Namuna tekshiruvi - bir xil ID lar bo'yicha
        const sampleIds = mysqlIds.slice(0, Math.min(50, mysqlIds.length)).map(row => row.id);
        if (sampleIds.length > 0) {
            const mysqlSampleQuery = `SELECT * FROM ${table} WHERE id IN (${sampleIds.join(',')}) ORDER BY id`;
            const pgSampleQuery = `SELECT * FROM ${table} WHERE id IN (${sampleIds.join(',')}) ORDER BY id`;
            const [mysqlSample] = await mysqlConn.execute(mysqlSampleQuery);
            const pgSample = await pgClient.query(pgSampleQuery);

            logToFile(`${table}: Namuna qatorlari - MySQL: ${mysqlSample.length}, PostgreSQL: ${pgSample.rows.length}`);
            if (mysqlSample.length !== pgSample.rows.length) {
                isMatch = false;
                mismatchReason = `Namuna qator soni farq qiladi: MySQL=${mysqlSample.length}, PostgreSQL=${pgSample.rows.length}`;
                console.log(`${table}: ${mismatchReason}`);
                logToFile(`${table}: ${mismatchReason}`);
                return { isMatch, mismatchReason };
            }

            // Har bir qatorni alohida taqqoslash
            for (let i = 0; i < mysqlSample.length; i++) {
                const mysqlRow = mysqlSample[i];
                const pgRow = pgSample.rows[i];
                const comparison = compareRows(mysqlRow, pgRow);
                if (!comparison.match) {
                    isMatch = false;
                    mismatchReason = comparison.reason;
                    console.log(`${table}: ${mismatchReason}`);
                    logToFile(`${table}: Qator ID=${mysqlRow.id} - MySQL: ${JSON.stringify(mysqlRow)}`);
                    logToFile(`${table}: Qator ID=${pgRow.id} - PostgreSQL: ${JSON.stringify(pgRow)}`);
                    logToFile(`${table}: ${mismatchReason}`);
                    return { isMatch, mismatchReason };
                }
            }
            console.log(`${table}: Namuna to'g'ri`);
            logToFile(`${table}: Namuna mos keladi`);
        } else {
            console.log(`${table}: Namuna yo'q (jadval bo'sh)`);
            logToFile(`${table}: Namuna yo'q (jadval bo'sh)`);
        }

        // Foreign key tekshiruvi (yangilangan qism)
        const fkMysql = foreignKeysMysql.filter(fk => fk.TABLE_NAME === table);
        const fkPg = foreignKeysPg.filter(fk => fk.TABLE_NAME === table);

        logToFile(`${table} - MySQL foreign keys: ${JSON.stringify(fkMysql)}`);
        logToFile(`${table} - PostgreSQL foreign keys: ${JSON.stringify(fkPg)}`);

        if (fkMysql.length !== fkPg.length) {
            isMatch = false;
            mismatchReason = `Foreign key soni farq qiladi: MySQL=${fkMysql.length} (${fkMysql.map(fk => `${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME}`).join(', ')}), PostgreSQL=${fkPg.length} (${fkPg.map(fk => `${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME}`).join(', ')})`;
            console.log(`${table}: ${mismatchReason}`);
            logToFile(`${table}: ${mismatchReason}`);
            return { isMatch, mismatchReason };
        }

        for (const fk of fkMysql) {
            const pgFk = fkPg.find(p => 
                p.COLUMN_NAME.toLowerCase() === fk.COLUMN_NAME.toLowerCase() && 
                p.REFERENCED_TABLE_NAME.toLowerCase() === fk.REFERENCED_TABLE_NAME.toLowerCase() &&
                p.REFERENCED_COLUMN_NAME.toLowerCase() === fk.REFERENCED_COLUMN_NAME.toLowerCase()
            );
            if (!pgFk) {
                isMatch = false;
                mismatchReason = `Foreign key topilmadi: ${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME} (${fk.REFERENCED_COLUMN_NAME})`;
                console.log(`${table}: ${mismatchReason}`);
                logToFile(`${table}: ${mismatchReason}`);
                return { isMatch, mismatchReason };
            }

            const [mysqlFkValues] = await mysqlConn.execute(`SELECT DISTINCT ${fk.COLUMN_NAME} FROM ${table} WHERE ${fk.COLUMN_NAME} IS NOT NULL`);
            const pgFkValues = await pgClient.query(`SELECT DISTINCT ${fk.COLUMN_NAME} FROM ${table} WHERE ${fk.COLUMN_NAME} IS NOT NULL`);
            const mysqlFkSet = new Set(mysqlFkValues.map(row => String(row[fk.COLUMN_NAME])));
            const pgFkSet = new Set(pgFkValues.rows.map(row => String(row[fk.COLUMN_NAME])));

            if (mysqlFkSet.size !== pgFkSet.size || ![...mysqlFkSet].every(val => pgFkSet.has(val))) {
                isMatch = false;
                mismatchReason = `Foreign key qiymatlari farq qiladi: ${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME}`;
                console.log(`${table}: ${mismatchReason}`);
                logToFile(`${table}: ${mismatchReason}`);
                return { isMatch, mismatchReason };
            }
            console.log(`${table}: Foreign key ${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME} to'g'ri`);
            logToFile(`${table}: Foreign key ${fk.COLUMN_NAME} -> ${fk.REFERENCED_TABLE_NAME} mos keladi`);
        }

        return { isMatch, mismatchReason };
    } catch (err) {
        isMatch = false;
        mismatchReason = `Xatolik yuz berdi: ${err.message}`;
        console.error(`${table}: ${mismatchReason}`);
        logToFile(`${table}: ${mismatchReason}`);
        return { isMatch, mismatchReason };
    }
}

async function checkDataIntegrity() {
    let mysqlConn, pgClient;
    try {
        mysqlConn = await mysql.createConnection(mysqlConfig);
        pgClient = new Client(pgConfig);
        await pgClient.connect();

        logToFile('Umumiy tekshiruv boshlandi');

        const foreignKeysMysql = await getForeignKeys(mysqlConn, 'mysql', mysqlConfig.database);
        const foreignKeysPg = await getForeignKeys(pgClient, 'postgres');

        logToFile(`MySQL foreign key soni: ${foreignKeysMysql.length}, PostgreSQL foreign key soni: ${foreignKeysPg.length}`);

        const [mysqlTables] = await mysqlConn.execute('SHOW TABLES;');
        const mysqlTableNames = mysqlTables.map(row => row[`Tables_in_${mysqlConfig.database}`]);
        const pgTables = await pgClient.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';");
        const pgTableNames = pgTables.rows.map(row => row.table_name);

        if (mysqlTableNames.length !== pgTableNames.length || mysqlTableNames.sort().join() !== pgTableNames.sort().join()) {
            console.log('Jadval ro\'yxatida farq bor!');
            logToFile('Jadval ro\'yxatida farq bor!');
            return;
        }
        console.log('Jadval soni to\'g\'ri');
        logToFile('Jadval soni mos keladi');

        for (const table of mysqlTableNames) {
            console.log(`\nTekshirilmoqda: ${table}`);
            logToFile(`${table}: Jadval tekshiruvi boshlandi`);
            const { isMatch, mismatchReason } = await checkSingleTable(table, mysqlConn, pgClient, foreignKeysMysql, foreignKeysPg);

            if (isMatch) {
                console.log(`✅ ${table}: Jadval mos keladi`);
                logToFile(`${table}: Jadval mos keladi`);
            } else {
                console.log(`❌ ${table}: Jadval mos emas - Sabab: ${mismatchReason}`);
                logToFile(`${table}: Jadval mos emas - Sabab: ${mismatchReason}`);
                console.log(`Xatolik aniqlandi! ${table} jadvalida muammo bor: ${mismatchReason}`);
            }

            logToFile(`${table}: Tekshiruv yakunlandi`);
            await sleep(1000);
        }

        console.log('\nBarcha tekshiruvlar yakunlandi.');
        logToFile('Barcha tekshiruvlar yakunlandi');
    } catch (error) {
        console.error('Umumiy xatolik:', error);
        logToFile(`Umumiy XATO: ${error.message}`);
    } finally {
        if (mysqlConn) await mysqlConn.end();
        if (pgClient) await pgClient.end();
    }
}

checkDataIntegrity();