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

// Vaqtni moslashtirish uchun yordamchi funksiya
function normalizeDate(value) {
    if (value === null) return 'NULL';
    const date = new Date(value);
    return date.toISOString().replace('T', ' ').substring(0, 19); // "YYYY-MM-DD HH:MM:SS"
}

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

        // Faqat vaqt ustunlari uchun normalizatsiya qilamiz
        if (key === 'created_at' || key === 'updated_at' || key === 'deleted_at' || key === 'date') { 
            mysqlValue = normalizeDate(mysqlValue);
            pgValue = normalizeDate(pgValue);
        } else {
            // Boshqa barcha qiymatlarni String sifatida taqqoslaymiz
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

async function checkSingleTable(table, mysqlConn, pgClient) {
    let isMatch = true;
    let mismatchReason = '';

    try {
        logToFile(`${table}: Tekshiruv boshlandi`);

        const [mysqlCount] = await mysqlConn.execute(`SELECT COUNT(*) as count FROM \`${table}\``);
        const pgCount = await pgClient.query(`SELECT COUNT(*) as count FROM "${table}"`);
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

        // ID tekshiruvi - agar jadvalda id bo‘lsa
        let mysqlIds, pgIds;
        try {
            [mysqlIds] = await mysqlConn.execute(`SELECT id FROM \`${table}\` ORDER BY id`);
            pgIds = await pgClient.query(`SELECT id FROM "${table}" ORDER BY id`);
        } catch (err) {
            logToFile(`${table}: ID tekshiruvi o‘tkazilmadi (id ustuni yo‘q yoki xatolik): ${err.message}`);
            mysqlIds = [];
            pgIds = { rows: [] };
        }

        const mysqlIdList = mysqlIds.map(row => String(row.id));
        const pgIdList = pgIds.rows.map(row => String(row.id));

        if (mysqlIdList.length > 0 && pgIdList.length > 0) {
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
        } else {
            logToFile(`${table}: ID lar tekshirilmadi (mavjud emas)`);
        }

        const sampleIds = mysqlIdList.slice(0, Math.min(50, mysqlIdList.length));
        if (sampleIds.length > 0) {
            const mysqlSampleQuery = `SELECT * FROM \`${table}\` WHERE id IN (${sampleIds.map(id => `'${id}'`).join(',')}) ORDER BY id`;
            const pgSampleQuery = `SELECT * FROM "${table}" WHERE id IN (${sampleIds.map(id => `'${id}'`).join(',')}) ORDER BY id`;
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
            console.log(`${table}: Namuna yo'q (jadval bo'sh yoki ID mavjud emas)`);
            logToFile(`${table}: Namuna yo'q (jadval bo'sh yoki ID mavjud emas)`);
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
            const { isMatch, mismatchReason } = await checkSingleTable(table, mysqlConn, pgClient); // foreignKeysMysql va foreignKeysPg olib tashlandi

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