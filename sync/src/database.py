from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
import os

Base = declarative_base()

class Image(Base):
    __tablename__ = 'images'
    
    id = Column(Integer, primary_key=True)
    uuid = Column(String, unique=True, nullable=True)
    url = Column(String, unique=True)
    local_path = Column(String)
    sha256_hash = Column(String)
    title = Column(String)
    author = Column(String)
    author_url = Column(String)
    license = Column(String)
    camera_make = Column(String)
    camera_model = Column(String)
    focal_length = Column(String)
    aperture = Column(String)
    shutter_speed = Column(String)
    taken_date = Column(String)
    page_url = Column(String)
    upload_date = Column(String)
    description = Column(Text)
    metadata_updated = Column(String)

    tags = relationship("Tag", secondary="image_tags", back_populates="images")
    albums = relationship("Album", secondary="image_albums", back_populates="images")
    collections = relationship("Collection", secondary="image_collections", back_populates="images")

class Tag(Base):
    __tablename__ = 'tags'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    count = Column(Integer)
    
    images = relationship("Image", secondary="image_tags", back_populates="tags")

class Album(Base):
    __tablename__ = 'albums'
    
    id = Column(Integer, primary_key=True)
    album_id = Column(String, unique=True)
    title = Column(String)
    url = Column(String)
    is_public = Column(Boolean)
    
    images = relationship("Image", secondary="image_albums", back_populates="albums")

class Collection(Base):
    __tablename__ = 'collections'
    
    id = Column(Integer, primary_key=True)
    collection_id = Column(String, unique=True)
    title = Column(String)
    url = Column(String)
    is_public = Column(Boolean)
    
    images = relationship("Image", secondary="image_collections", back_populates="collections")

class ImageTag(Base):
    __tablename__ = 'image_tags'
    
    image_id = Column(Integer, ForeignKey('images.id'), primary_key=True)
    tag_id = Column(Integer, ForeignKey('tags.id'), primary_key=True)

class ImageAlbum(Base):
    __tablename__ = 'image_albums'
    
    image_id = Column(Integer, ForeignKey('images.id'), primary_key=True)
    album_id = Column(Integer, ForeignKey('albums.id'), primary_key=True)

class ImageCollection(Base):
    __tablename__ = 'image_collections'
    
    image_id = Column(Integer, ForeignKey('images.id'), primary_key=True)
    collection_id = Column(Integer, ForeignKey('collections.id'), primary_key=True)

class SyncRecord(Base):
    __tablename__ = 'sync_records'
    
    id = Column(Integer, primary_key=True)
    image_id = Column(Integer, ForeignKey('images.id'))
    remote_source = Column(String)  # Identifier for the remote source
    sync_date = Column(DateTime, default=datetime.utcnow)
    remote_path = Column(String)  # Original path in remote system
    status = Column(String)  # 'success', 'failed', 'pending'
    error_message = Column(Text, nullable=True)

def init_db(db_path: str):
    """Initialize database connection and create tables if they don't exist"""
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)() 