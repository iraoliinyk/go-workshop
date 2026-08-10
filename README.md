# Go Workshop

This repository contains the code for a Go backend engineering workshop.

## Structure

The workshop is split into chapters. Each chapter lives in its own directory
(`ch-1`, `ch-2`, and so on) and runs independently, so you can work on one
chapter without setting up the others.

Each chapter directory also has its own README file, named after the chapter
(for example, `ch-2/CH2_README.md`). It explains how to run and test that
chapter, so read it before you start.

## Environment variables

The `.env` file is not committed to version control, but `.env.example` is. To
run a chapter locally, go into that chapter's directory and copy the example
file first:

```bash
cp .env.example .env
```

## What each chapter adds

- From `ch-2` and later, a chapter also includes a `docker-compose.yaml` file.
- From `ch-3` and later, a chapter also includes a Postman collection.
