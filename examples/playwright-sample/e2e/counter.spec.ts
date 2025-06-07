import { test, expect } from '@playwright/test';

test.describe.configure({mode: "serial"})

test('initial status', async ({ page, request }) => {
  await request.post("http://localhost:8000/api/seed/initial.yaml")
  await page.goto('/');
  await expect(page.getByRole('status', { name: 'Count' })).toHaveText("0")
});

test('count up', async({page, request}) => {
  await request.post("http://localhost:8000/api/seed/initial.yaml")
  await page.goto('/');
  await page.getByRole('button', { name: 'Count' }).click();
  await expect(page.getByRole('status', { name: 'Count' })).toHaveText("1")
})

test('overflow status', async ({ page, request }) => {
  await request.post("http://localhost:8000/api/seed/overflow.yaml")
  await page.goto('/');
  await expect(page.getByText('Error: HTTP error! status:')).toBeVisible()
});
