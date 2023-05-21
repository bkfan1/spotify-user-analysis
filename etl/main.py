from etl_process import extract, transform, load


def main():
    # Extract
    raw_data = extract()

    # Transform
    transformed_data = transform(raw_data)

    # Load
    load(transformed_data)


if __name__ == "__main__":
    main()
