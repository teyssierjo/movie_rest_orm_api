from typing import List, Optional, Tuple
import logging

from fastapi import Depends, FastAPI, HTTPException
from fastapi.logger import logger as fastapi_logger
from sqlalchemy.orm import Session

import crud, models, schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)


app = FastAPI()

logger = logging.getLogger("uvicorn")
fastapi_logger.handlers = logger.handlers
fastapi_logger.setLevel(logger.level)
logger.error("API Started")


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


################################################################################
#                                                                              #
#                                    MOVIES                                    #
#                                                                              #
################################################################################

# READ ALL MOVIES
@app.get("/movies/", response_model=List[schemas.Movie])
def get_movies(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # read movies from database
    movies = crud.get_movies(db, skip=skip, limit=limit)
    # return them as json
    return movies

################################################################################

# READ ONE MOVIE BY ITS ID
@app.get("/movies/by_id/id/{movie_id}", response_model=schemas.Movie)
def get_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.get_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to read not found")
    return db_movie

################################################################################

# READ MOVIES BY THEIR ENTIRE TITLE
@app.get("/movies/by_title", response_model=List[schemas.Movie])
def get_movies_by_title(n: Optional[str] = None, db: Session = Depends(get_db)):
    # read items from database
    movies = crud.get_movies_by_title(db=db, title=n)
    # return them as json
    return movies

################################################################################

# READ MOVIES BY A PART OF THEIR TITLE
@app.get("/movies/by_parttitle", response_model=List[schemas.Movie])
def get_movies_by_parttitle(n: Optional[str] = None, db: Session = Depends(get_db)):
    movies = crud.get_movies_by_parttitle(db=db, title=n)
    return movies

################################################################################

# READ ALL MOVIES FROM AN INTERVAL OF YEARS [Ymin-Ymax]
@app.get("/movies/by_year", response_model=List[schemas.Movie])
def get_movies_by_year(ymin: Optional[float] = None, ymax: Optional[float] = None, db: Session = Depends(get_db)):
    # read movies from database
    movies = crud.get_movies_by_range_year(db=db, year_min=ymin, year_max=ymax)
    if movies is None:
        raise HTTPException(status_code=404, detail="Movie for empty range year not found")
    # return them as json
    return movies

################################################################################

# ADD/CREATE A MOVIE
@app.post("/movies/", response_model=schemas.Movie)
def create_movie(movie: schemas.MovieCreate, db: Session = Depends(get_db)):
    # receive json movie without id and return json movie from database with new id
    return crud.create_movie(db=db, movie=movie)

################################################################################

# UPDATE A MOVIE
@app.put("/movies/", response_model=schemas.Movie)
def update_movie(movie: schemas.Movie, db: Session = Depends(get_db)):
    db_movie = crud.update_movie(db, movie=movie)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to update not found")
    return db_movie

################################################################################

# DELETE A MOVIE
@app.delete("/movies/by_id/{movie_id}", response_model=schemas.Movie)
def delete_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.delete_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to delete not found")
    return db_movie



################################################################################
#                                                                              #
#                                     STARS                                    #
#                                                                              #
################################################################################

# READ ALL STARS
@app.get("/stars/", response_model=List[schemas.Star])
def get_stars(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    # read all stars from database
    stars = crud.get_stars(db, skip=skip, limit=limit)
    # return them as json
    return stars

################################################################################

# READ ONE STAR BY ITS ID
@app.get("/stars/id/by_id/{star_id}", response_model=schemas.Star)
def get_star(star_id: int, db: Session = Depends(get_db)):
    # read a star by id from database
    db_star = crud.get_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to read not found")
    # return them as json
    return db_star

################################################################################

# READ STARS BY THEIR ENTIRE NAME
@app.get("/stars/by_name", response_model=List[schemas.Star])
def get_stars_by_name(n: Optional[str] = None, db: Session = Depends(get_db)):
    # read stars by name from database
    stars = crud.get_stars_by_name(db=db, name=n)
    # return them as json
    return stars

################################################################################

# READ STARS BY A PART OF THEIR NAME
@app.get("/stars/by_partname", response_model=List[schemas.Star])
def get_stars_by_partname(n: Optional[str] = None, db: Session = Depends(get_db)):
    # read stars by partname from database
    stars = crud.get_stars_by_partname(db=db, name=n)
    # return them as json
    return stars

################################################################################

# READ STARS BY THEIR BIRTHYEAR
@app.get("/stars/by_birthyear/{year}", response_model=List[schemas.Star])
def get_stars_by_birthyear(year: int, db: Session = Depends(get_db)):
    # read stars by birthyear from database
    stars = crud.get_stars_by_birthyear(db=db, year=year)
    # return them as json
    return stars

################################################################################

# ADD/CREATE A STAR
@app.post("/stars/", response_model=schemas.Star)
def create_star(star: schemas.StarCreate, db: Session = Depends(get_db)):
    # receive json star without id and return json star from database with new id
    return crud.create_star(db=db, star=star)

################################################################################

# UPDATE A STAR
@app.put("/stars/", response_model=schemas.Star)
def update_star(star: schemas.Star, db: Session = Depends(get_db)):
    # update a star
    db_star = crud.update_star(db, star=star)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to update not found")
    # return it as json
    return db_star

################################################################################

# DELETE A STAR
@app.delete("/stars/by_id/{star_id}", response_model=schemas.Star)
def delete_star(star_id: int, db: Session = Depends(get_db)):
    # delete a star
    db_star = crud.delete_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to delete not found")
    # return it as json
    return db_star



################################################################################
#                                                                              #
#                              DIRECTORS & ACTORS                              #
#                                                                              #
################################################################################

@app.get("/movies/by_director", response_model=List[schemas.Movie])
def get_movies_by_director(n: str, db: Session = Depends(get_db)):
    return crud.get_movies_by_director_endname(db=db, endname=n)

################################################################################

@app.get("/movies/by_actor", response_model=List[schemas.Movie])
def get_movies_by_actor(n: str, db: Session = Depends(get_db)):
    return crud.get_movies_by_actor_endname(db=db, endname=n)

################################################################################

@app.get("/stars/by_id/{movie_id}", response_model=schemas.Star)
def get_director_by_movie_id(movie_id: int, db: Session = Depends(get_db)):
    director = crud.get_director_by_movie_id(db=db, movie_id=movie_id)
    if director is None:
        raise HTTPException(status_code=404, detail="No director for this movie")
    return director

################################################################################

@app.get("/stars/by_movie_endname", response_model=List[schemas.Star])
def get_actors_by_movie_endname(n: Optional[str] = None, db: Session = Depends(get_db)):
    actors = crud.get_actors_by_movie_endname(db=db, endname=n)
    if actors is None:
        raise HTTPException(status_code=404, detail="No actor for this movie")
    return actors

################################################################################

@app.get("/stars/by_movie_id", response_model=List[schemas.Star])
def get_actors_by_movie_id(movie_id: int, db: Session = Depends(get_db)):
    actors = crud.get_actors_by_movie_id(db=db, movie_id=movie_id)
    if actors is None:
        raise HTTPException(status_code=404, detail="No actor for this movie")
    return actors

################################################################################

@app.put("/movies/director/", response_model=schemas.MovieDetail)
def update_movie_director(mid: int, sid: int, db: Session = Depends(get_db)):
    db_movie = crud.update_movie_director(db=db, movie_id=mid, director_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Director not found")
    return db_movie

################################################################################

@app.post("/movies/actor/")
def add_movie_actor(mid: int, sid: int, db: Session = Depends(get_db)):
    db_movie = crud.add_movie_actor(db=db, movie_id=mid, star_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Actor not found")
    return db_movie

################################################################################

@app.put("/movies/actors", response_model=schemas.MovieDetail)
def update_movie_actors(mid: int, sids: List[int], db: Session = Depends(get_db)):
    db_movie = crud.update_movie_actors(db=db, movie_id=mid, star_ids=sids)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie



################################################################################
#                                                                              #
#                                  STATISTICS                                  #
#                                                                              #
################################################################################

# COUNT MOVIES
@app.get("/movies/count")
def get_movies_count(db: Session = Depends(get_db)):
    return crud.get_movies_count(db=db)

################################################################################

# COUNT MOVIES BY A YEAR GIVEN
@app.get("/movies/count/{year}")
def get_movies_count_year(year:int, db: Session = Depends(get_db)):
    return crud.get_movies_count_year(db=db, year=year)

################################################################################

# COUNT STARS
@app.get("/stars/count")
def get_stars_count(db: Session = Depends(get_db)):
    return crud.get_stars_count(db=db)

################################################################################

# COUNT MOVIES FOR EACH YEAR
@app.get("/movies/count_by_year")
def get_movies_count_by_year(db: Session=Depends(get_db)) -> List[Tuple[int,int,int,int,int]]:
    return crud.get_movies_count_by_year(db=db)

################################################################################

# STATS MOVIES FOR EACH YEAR
@app.get("/movies/stats_by_year")
def get_movies_stats_by_year(db: Session=Depends(get_db)) -> List[Tuple[int,int,int,int,int]]:
    return crud.get_movies_stats_by_year(db=db)

################################################################################

# STATS MOVIES DICTIONNARY FOR EACH YEAR:
#   - count movies
#   - min duration
#   - max duration
#   - average duration
@app.get("/movies/stats_by_year_dict")
def get_movies_stats_by_year_dict(db: Session = Depends(get_db)) -> List[schemas.MovieStat]:
    return crud.get_movies_stats_by_year_dict(db=db)

################################################################################

# STATS MOVIES BY DIRECTOR
@app.get("/stars/stats_movie_by_director")
def get_stats_movie_by_director(minc: Optional[int] = 10, db: Session = Depends(get_db)):
    return crud.get_stats_movie_by_director(db=db, min_count=minc)

################################################################################

# STATS MOVIES BY ACTOR
@app.get("/stars/stats_movie_by_actor")
def get_stats_movie_by_actor(minc: Optional[int] = 10, db: Session = Depends(get_db)) -> List[schemas.MovieStat]:
    return crud.get_stats_movie_by_actor(db=db, min_count=minc)
