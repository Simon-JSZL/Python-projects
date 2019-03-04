import pygame
import game_functions as gf
from setting import Settings
from ship import Ship
from pygame.sprite import Group


def run_game():
    pygame.init()
    ai_setting = Settings()
    screen = pygame.display.set_mode((ai_setting.screen_width, ai_setting.screen_height))
    pygame.display.set_caption("Alien_Invasion")
    ship = Ship(screen, ai_setting)
    bullets = Group()
    aliens = Group()
    gf.create_fleet(ai_setting, screen, aliens)
    while True:
        gf.check_events(ai_setting, screen, ship, bullets)
        ship.update()
        bullets.update()
        gf.update_screen(ai_setting, screen, ship, bullets, aliens)


run_game()

