# Web Scrapping Project

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed on your machine.
- [Docker Compose](https://docs.docker.com/compose/) (included with Docker Desktop).

## Running the Project

1. **Clone the repository** (if you haven't already):

    ```sh
    git clone <your-repo-url>
    cd <project-directory>
    ```

2. **Start the services using Docker Compose:**

    ```sh
    docker-compose up -d
    ```

    This will start Zookeeper and Kafka containers as defined in `docker-compose.yml`.

3. **Verify the containers are running:**

    ```sh
    docker ps
    ```

    You should see containers for both `zookeeper` and `kafka`.

4. **Stop the services:**

    ```sh
    docker-compose down
    ```

## Notes

- Kafka will be accessible at `localhost:29092`.
- Zookeeper will be accessible at `localhost:22181`.
- Make sure to update any application code to use these ports if needed.

## Additional Steps

- Add any project-specific setup or usage instructions here (e.g., how to run your web scraping scripts, dependencies, etc.).
