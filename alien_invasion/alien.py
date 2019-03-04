import pygame
from pygame.sprite import Sprite


class Alien(Sprite):
    def __init__(self, ai_setting, screen):
        super().__init__()
        self.ai_setting = ai_setting
        self.screen = screen
        self.image = pygame.image.load('images/alien.bmp')
        self.rect = self.image.get_rect()

        self.rect.centerx = self.rect.width
        self.rect.centery = self.rect.height
        self.x = float(self.rect.centerx)

