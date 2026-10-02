import pg from 'pg';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '../generated/prisma/index.js';
import { createPgPool, getPoolMetrics } from './pg-pool.js';

const globalForPrisma = global as unknown as {
  prisma?: PrismaClient;
  pool?: pg.Pool;
};

if (!globalForPrisma.pool) {
  globalForPrisma.pool = createPgPool();
}

// PrismaPg accepts a pg.Pool directly; the `pg` copy resolved here can differ
// from the adapter's own, which is harmless at runtime because the adapter only
// duck-types the pool it is handed.
const adapter = new PrismaPg(globalForPrisma.pool);

export const prisma =
  globalForPrisma.prisma ||
  new PrismaClient({
    adapter,
    log: process.env.NODE_ENV === 'development' ? ['query', 'error', 'warn'] : ['error'],
  });

if (process.env.NODE_ENV !== 'production') globalForPrisma.prisma = prisma;

export { getPoolMetrics };
export const pool = globalForPrisma.pool!;

export default prisma;
