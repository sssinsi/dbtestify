import { NextResponse } from 'next/server';
import prisma from '@/lib/prisma';

export async function GET() {
    try {
        let counter = await prisma.counter.findFirst();

        if (!counter) {
            counter = await prisma.counter.create({
                data: { value: 0 },
            });
        }

        return NextResponse.json({ value: counter.value });
    } catch (error) {
        console.error('Error fetching counter:', error);
        return NextResponse.json({ error: 'Failed to fetch counter' }, { status: 500 });
    }
}

export async function POST(req: Request) {
    try {
        const body = await req.json();
        const incrementBy = body.incrementBy || 1;

        let counter = await prisma.counter.findFirst();

        if (!counter) {
            counter = await prisma.counter.create({
                data: { value: incrementBy },
            });
        } else {
            counter = await prisma.counter.update({
                where: { id: counter.id },
                data: { value: { increment: incrementBy } },
            });
        }
        return NextResponse.json({ value: counter.value });
    } catch (error) {
        console.error('Error updating counter:', error);
        return NextResponse.json({ error: 'Failed to update counter' }, { status: 500 });
    }    
}