# Cloud Assignment

## How to install the API server

### Install virtualenv
```
pip install virtualenv
```

### Create it
```
virtualenv .venv
```

### Activate it
```
.\venv\Scripts\activate
```

### Install requirements
```
pip install requirements.txt
```

## Running the API server
```
uvicorn main:app --reload
```