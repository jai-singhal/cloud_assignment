# Cloud Assignment

## How to install the API server

### Install virtualenv
```
pip install virtualenv
```

#### Linux 

```
pip3 install virtualenv
```

### Create it
```
virtualenv .venv
```

#### Linux 

```
virtualenv -p python3 .venv
```

### Activate it
```
.\venv\Scripts\activate
```

#### Linux 

```
source .venv/bin/activate
```


### Install requirements
```
pip install -r requirements.txt
```

#### Linux 

```
pip3 install -r requirements.txt
```

## Running the API server

```
uvicorn api:app --reload
```